using System.Globalization;
using System.IO;
using System.Net.Http.Headers;
using System.Net.NetworkInformation;
using System.Xml.Linq;

namespace CTree
{
    public enum CTreeAddressing
    {
        x32bit,
        x64bit
    }

    /// <summary>
    /// Base class for a CTree. Implement your own tree by simply inherit this.
    /// </summary>
    public class CTree
    {
        private struct CNode
        {
            public long[] Addresses { get; set; }
            public long ContentAddress { get; set; }
            public int ContentLength { get; set; }
        }

        private int _longLength;
        private readonly CTreeAddressing _addressing;
        private string _path;
        private string _occurringLetters;
        private long _fileSize;
        private Dictionary<char, int> _lookup;
        private Dictionary<int, char> _lookupBackwards;
        private int _numLookupChars;
        private int _maxMegabyteInBuffer;
        private int _maxCacheMegabyte;
        private int _numBytesInHoles;

        /// <summary>
        /// Constructor.
        /// Note that the to maxMegabyte parameters can be played with depending on your client's specs. 20/20 MB would probably cut it for most cases. These two
        /// parameters can be changed even after the .ctree file has been created the first time.
        /// </summary>
        /// <param name="path">The path to where the .ctree-file is stored.</param>
        /// <param name="occurringLetters">Which characters to include. Note, this cannot be changed once the file has been created! If only numeric values are used as key, this should typically be 0123456789.</param>
        /// <param name="maxMegabyteInBuffer">The internal buffer during a Bulk. New data is appended to this buffer until size > maxMegabyteInBuffer. Then the content is flushed to disk.
        /// <param name="maxCacheMegabyte">During a Bulk, some data is cached. When the total bytes of this cache > maxCacheMegabyte, the content is flushed to disk. 
        protected CTree(CTreeAddressing addressing, string path, string occurringLetters, int maxMegabyteInBuffer = 20, int maxCacheMegabyte = 20)
        {
            _occurringLetters = occurringLetters;
            _maxMegabyteInBuffer = maxMegabyteInBuffer;
            _maxCacheMegabyte = maxCacheMegabyte;
            _bulkBuffer = new byte[_maxMegabyteInBuffer * 1000000];
            _addressing = addressing;
            _path = path;
            int i = 0;
            _lookup = new Dictionary<char, int>();
            _lookupBackwards = new Dictionary<int, char>();
            foreach (var l in occurringLetters.ToCharArray())
            {
                if (!_lookup.ContainsKey(l))
                {
                    _lookup.Add(l, i);
                    _lookupBackwards.Add(i, l);
                    i++;
                }
                _numLookupChars = i;
            }

            if (_addressing == CTreeAddressing.x32bit)
            {
                _longLength = 4;
            }
            else
            {
                _longLength = 8;
            }
        }

        private int _bufferLengthCache = 0;
        private int GetBufferLength()
        {
            if ( _bufferLengthCache == 0)
                _bufferLengthCache = (_numLookupChars * _longLength) + (_longLength + 4);
            return _bufferLengthCache;
        }

        private static int GetIntFromByteArray(byte[] barr, int startIndex)
        {
            return BitConverter.ToInt32(barr, startIndex);
        }

        private static byte[] GetByteArrayFromInt(int i)
        {
            return BitConverter.GetBytes(i);
        }

        private long GetLongFromByteArray(byte[] barr, int startIndex)
        {
            if (_addressing == CTreeAddressing.x32bit)
            {
                var retValAsInt = BitConverter.ToInt32(barr, startIndex);
                return (long)retValAsInt;
            }
            else
                return BitConverter.ToInt64(barr, startIndex);
        }

        private byte[] GetByteArrayFromLong(long i)
        {
            if (_addressing == CTreeAddressing.x32bit)
            {
                var iAsInt = (int) i;
                return BitConverter.GetBytes(iAsInt);   
            }
            else
                return BitConverter.GetBytes(i);
        }

        private CNode CreateCNode(byte[] barr)
        {
            var retObj = new CNode();
            retObj.Addresses = new long[_numLookupChars];
            for (var i = 0; i < _numLookupChars; i++)
            {
                retObj.Addresses[i] = GetLongFromByteArray(barr, i * _longLength);
            }
            retObj.ContentAddress = GetLongFromByteArray(barr, (_numLookupChars + 0) * _longLength);
            retObj.ContentLength = GetIntFromByteArray(barr, (_numLookupChars + 1) * _longLength);
            return retObj;
        }

        private byte[] GetBuffer(CNode node)
        {
            var retBuf = new byte[GetBufferLength()];
            for (int i = 0; i < _numLookupChars; i++)
            {
                Array.Copy(GetByteArrayFromLong(node.Addresses[i]), 0, retBuf, i * _longLength, _longLength);
            }
            Array.Copy(GetByteArrayFromLong(node.ContentAddress), 0, retBuf, (_numLookupChars + 0) * _longLength, _longLength);
            Array.Copy(GetByteArrayFromInt(node.ContentLength), 0, retBuf, (_numLookupChars + 1) * _longLength, 4);

            return retBuf;
        }

        private int GetIndexForChar(char ch)
        {
            return _lookup[ch];
        }

        private char GetCharFromIndex(int i)
        {
            return _lookupBackwards[i];
        }


        private long AppendBuffer(FileStream fs, byte[] buffer, int length)
        {
            fs.Seek(0, SeekOrigin.End);
            var addr = fs.Position;

            fs.Write(buffer, 0, length);
            fs.Flush();
            return _fileSize + length;
        }

        private long AppendValue(FileStream fs, byte[] buffer)
        {
            if (_isInBulk)
            {
                // If the new buffer would yield in more memory than _bulkBufferLength - just flush to disk
                // and continue with empty _bulkBuffer and updated _fileSize!
                if (_bulkBufferLength + buffer.Length >= _bulkBuffer.Length)
                {
                    FlushBulk(true);
                }


                var addr = _fileSize + _bulkBufferLength;
                // Add the new buffer to bulkBuffer
                Array.Copy(buffer, 0, _bulkBuffer, _bulkBufferLength, buffer.Length);
                _bulkBufferLength += buffer.Length;
                return addr;
            }
            else
            {
                return AppendBuffer(fs, buffer, buffer.Length);
            }
        }

        private long AppendCNode(FileStream fs, CNode node)
        {
            var buffer = GetBuffer(node);

            if (_isInBulk)
            {
                // If the new buffer would yield in more memory than _bulkBufferLength - just flush to disk
                // and continue with empty _bulkBuffer and updated _fileSize!
                if (buffer.Length + _bulkBufferLength >= _bulkBuffer.Length)
                {
                    FlushBulk(true);
                }

                var addr = _fileSize + _bulkBufferLength;
                // Add the new buffer to bulkBuffer
                Array.Copy(buffer, 0, _bulkBuffer, _bulkBufferLength, buffer.Length);
                _bulkBufferLength += buffer.Length;
                // Add this node to the ones being appended during bulk!
                _nodesInBulkThatHaveBeenAppended.Add(addr, node);
                return (dynamic)addr;
            }
            else
                return AppendBuffer(fs, buffer, buffer.Length);
        }

        private void ReWriteBuffer(FileStream fs, long addr, byte[] buffer)
        {
            fs.Seek(addr, SeekOrigin.Begin);
            fs.Write(buffer, 0, buffer.Length);
            fs.Flush();
        }

        private void ReWriteValue(FileStream fs, long addr, byte[] buffer)
        {
            if (_isInBulk)
            {
                if (_valuesInFileNeedingUpdateAfterBulk.ContainsKey(addr))
                {
                    // We will rewrite a node that has already been. This will only occur if the same key is updated
                    // twice with different values...
                    _valuesInFileNeedingUpdateAfterBulk[addr] = buffer;
                }
                else
                {
                    // This node is apparently already in the file before bulk started, so add him there.
                    _valuesInFileNeedingUpdateAfterBulk.Add(addr, buffer);
                }
            }
            else
                ReWriteBuffer(fs, addr, buffer);
        }

        private void ReWriteCNode(FileStream fs, long addr, CNode node)
        {
            if (_isInBulk)
            {
                if (_nodesInBulkThatHaveBeenAppended.ContainsKey(addr))
                {
                    // We will rewrite a node that has already been
                    _nodesInBulkThatHaveBeenAppended[addr] = node;
                }
                else if (_nodesInFileNeedingUpdateAfterBulk.ContainsKey(addr))
                {
                    // This node was in file before bulk started, so we need to update him when bulk stops!
                    _nodesInFileNeedingUpdateAfterBulk[addr] = node;
                }
                else
                {
                    // This node is apparently already in the file before bulk started, so add him there.
                    _nodesInFileNeedingUpdateAfterBulk.Add(addr, node);
                    // Also, if this node was present in the dictorary for untouched nodes, remove it from there!
                    if (_nodesInFileButAreUnchanged.ContainsKey(addr))
                        _nodesInFileButAreUnchanged.Remove(addr);
                }
            }
            else
            {
                var buffer = GetBuffer(node);
                ReWriteBuffer(fs, addr, buffer);
            }
        }

        private CNode ReadCNode(FileStream fs, long addr)
        {
            if (_isInBulk)
            {
                // If we are in bulk and have seen this node before, just return it from
                // either of the three dictionaries.
                if (_nodesInBulkThatHaveBeenAppended.ContainsKey(addr))
                    return _nodesInBulkThatHaveBeenAppended[addr];
                if (_nodesInFileNeedingUpdateAfterBulk.ContainsKey(addr))
                    return _nodesInFileNeedingUpdateAfterBulk[addr];
                if (_nodesInFileButAreUnchanged.ContainsKey(addr))
                    return _nodesInFileButAreUnchanged[addr];
            }

            var bufferLength = GetBufferLength();
            var buffer = new byte[bufferLength];
            fs.Seek(addr, SeekOrigin.Begin);
            fs.Read(buffer, 0, bufferLength);
            var retObj = CreateCNode(buffer);

            if (_isInBulk)
            {
                // For performance enhancements, add this node in the dictionary for unchanged nodes.
                // ... it will be moved into file/changed-dictionary later in case it actually changes.
                _nodesInFileButAreUnchanged.Add(addr, retObj);
            }

            return retObj;
        }

        private byte[] ReadValue(FileStream fs, long address, int length)
        {
            var buffer = new byte[length];
            fs.Seek(address, SeekOrigin.Begin);
            fs.Read(buffer, 0, length);
            return buffer;
        }

        /// <summary>
        /// Insert/update a value to the tree file.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        protected void SetInternal(string key, byte[] value)
        {
            if (_isInBulk)
            {
                // Note, if the _bulkBuffer gets overflown, we will flush during AppendCNode or AppendValue... 

                // If the cache dictionaries exceeds the max wanted cache size - flush!
                var numNodesInCache = _nodesInBulkThatHaveBeenAppended.Count + _nodesInFileButAreUnchanged.Count + _nodesInFileNeedingUpdateAfterBulk.Count;
                var totalBytesInCache = numNodesInCache * GetBufferLength() / 1000000;
                if (totalBytesInCache > _maxCacheMegabyte * 1000000)
                {
                    FlushBulk(true);
                }

                Traverse(_fsForBulk, key.ToLower(), value, true);
            }
            else
            {
                throw new Exception("You need to be in Bulk mode when setting data");
            }
        }

        public long GetFileSizeInBytes()
        {
            if (_isInBulk)
            {
                throw new Exception("File size cannot be read during bulk");
            }
            Close();
            var fi = new FileInfo(_path);
            _fileSize = fi.Length;
            return _fileSize;
        }


        public int GetNumberOfBytesInHoles()
        {
            if (_isInBulk)
            {
                throw new Exception("File size cannot be read during bulk");
            }
            Close();
            _fsForRead = new FileStream(_path, FileMode.Open, FileAccess.Read);
            var barr = ReadValue(_fsForRead, 0, 4);
            var holes = GetIntFromByteArray(barr, 0);
            return holes;
        }

        private void VerifyFile()
        {
            if (!File.Exists(_path))
            {
                using (var ss = File.Create(_path))
                {
                    AppendBuffer(ss, GetByteArrayFromInt(0), 4);
                    ss.Close();
                }
            }
        }


        private FileStream _fsForRead = null;

        /// <summary>
        /// Gets a value from the tree file based on key.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        protected byte[] GetInternal(string key)
        {
            if (_fsForRead == null)
            {
                _fsForRead = new FileStream(_path, FileMode.Open, FileAccess.Read);
            }

            //using (var fs = new FileStream(_path, FileMode.Open, FileAccess.Read))
            {
                return Traverse(_fsForRead, key.ToLower(), null, false);
            }
        }

        private bool _isInBulk = false;
        private Dictionary<long, CNode> _nodesInFileNeedingUpdateAfterBulk = null; // Nodes that were present in file before Bulk started.
        private Dictionary<long, CNode> _nodesInBulkThatHaveBeenAppended = null; // New nodes that have been added during Bulk.
        private Dictionary<long, CNode> _nodesInFileButAreUnchanged = null; // Nodes that have been visited during bulk...
        private Dictionary<long, byte[]> _valuesInFileNeedingUpdateAfterBulk = null;

        private FileStream _fsForBulk;
        private byte[] _bulkBuffer;
        private int _bulkBufferLength;
        private int _numBytesInHolesInBulk;

        protected void StartBulkInternal()
        {
            VerifyFile();
            int numRetries = 0;
        @retry:
            try
            {
                RestartBulk();
                var barr = ReadValue(_fsForBulk, 0, 4);
                _numBytesInHoles = GetIntFromByteArray(barr, 0);
            }
            catch
            {
                // Do some retries.. just in case the file handles are slow to release...
                numRetries++;
                if (numRetries > 20)
                    throw;

                if (_fsForBulk != null)
                {
                    _fsForBulk.Close();
                    _fsForBulk.Dispose();
                    _fsForBulk = null;
                }

                Thread.Sleep(100);
                goto @retry;
            }

        }

        private void RestartBulk()
        {
            if (_fsForRead != null)
            {
                _fsForRead.Close();
                _fsForRead.Dispose();
                _fsForRead = null;
            }

            if (_fsForBulk != null)
            {
                _fsForBulk.Close();
                _fsForBulk.Dispose();
                _fsForBulk = null;
            }

            _nodesInFileNeedingUpdateAfterBulk = new Dictionary<long, CNode>();
            _nodesInBulkThatHaveBeenAppended = new Dictionary<long, CNode>();
            _nodesInFileButAreUnchanged = new Dictionary<long, CNode>();
            _valuesInFileNeedingUpdateAfterBulk = new Dictionary<long, byte[]>();
            _isInBulk = true;
            _bulkBufferLength = 0;
            _fsForBulk = new FileStream(_path, FileMode.Open, FileAccess.ReadWrite);
            var fi = new FileInfo(_path);
            _fileSize = fi.Length;
        }

        public void Close()
        {
            if (_fsForRead != null)
            {
                _fsForRead.Close();
                _fsForRead.Dispose();
                _fsForRead = null;
            }

            if (_fsForBulk != null)
            {
                _fsForBulk.Close();
                _fsForBulk.Dispose();
                _fsForBulk = null;
            }
        }

        private void FlushBulk(bool doContinue)
        {
            // Count number of holes we have created.
            if (_numBytesInHolesInBulk > 0)
            {
                _numBytesInHoles += _numBytesInHolesInBulk;
                _numBytesInHolesInBulk = 0;
                var barr = GetByteArrayFromInt(_numBytesInHoles);
                ReWriteBuffer(_fsForBulk, 0, barr);
            }

            // Loop through all newly appended nodes and adjust the bulkBuffer accordingly.
            foreach (var nodeTup in _nodesInBulkThatHaveBeenAppended)
            {
                var addr = nodeTup.Key;
                var node = nodeTup.Value;
                var buff = GetBuffer(node);
                var indexInBulkArray = addr - _fileSize;

                Array.Copy(buff, 0, _bulkBuffer, indexInBulkArray, buff.Length);
            }

            // BulkBuffer is now alright... just append it to the end of the file!
            AppendBuffer(_fsForBulk, _bulkBuffer, _bulkBufferLength);

            // And now, we need to adjust all nodes that were in the file previously and have been changed:
            foreach (var nodeTup in _nodesInFileNeedingUpdateAfterBulk)
            {
                ReWriteBuffer(_fsForBulk, nodeTup.Key, GetBuffer(nodeTup.Value));
            }

            // And now, all values that have been updated during bulk, rewrite them!
            foreach (var valTup in _valuesInFileNeedingUpdateAfterBulk)
            {
                ReWriteBuffer(_fsForBulk, valTup.Key, valTup.Value);
            }

            _nodesInFileNeedingUpdateAfterBulk.Clear();
            _nodesInBulkThatHaveBeenAppended.Clear();
            _nodesInFileButAreUnchanged.Clear();
            _valuesInFileNeedingUpdateAfterBulk.Clear();
            _bulkBufferLength = 0;

            _fsForBulk.Close();
            _fsForBulk.Dispose();
            _fsForBulk = null;
            if (doContinue)
                RestartBulk();
        }

        protected void StopBulkInternal()
        {
            FlushBulk(false);
            _isInBulk = false;
        }

        private byte[] Traverse(FileStream fs, string key, byte[] value, bool isUpdate)
        {
            CNode currentNode;
            long currentAddress = 4;
            if (_fileSize <= 4 && _bulkBufferLength == 0)
            {
                if (!isUpdate)
                {
                    // If file is 0 bytes, then we are probably not going to find this key here...
                    return null;
                }

                var barr = new byte[GetBufferLength()];
                for (var i = 0; i < barr.Length; i++)
                {
                    barr[i] = 0;
                }

                currentNode = CreateCNode(barr);
                AppendCNode(fs, currentNode);
            }
            else
            {
                currentNode = ReadCNode(fs, 4);
            }

            for (var strIndex = 0; strIndex < key.Length; strIndex++)
            {
                var currentChar = key[strIndex];
                var addrIndex = GetIndexForChar(currentChar);

                if (currentNode.Addresses[addrIndex] == 0)
                {
                    if (!isUpdate)
                    {
                        // This key does not exist apparently!
                        return null;
                    }

                    var barr = new byte[GetBufferLength()];
                    var tempNode = CreateCNode(barr);
                    var newAddr = AppendCNode(fs, tempNode);
                    currentNode.Addresses[addrIndex] = newAddr;
                    // New address has been added to currentNode. Need to save it!
                    ReWriteCNode(fs, currentAddress, currentNode);
                }

                // Now let's jump to the next level..
                currentAddress = currentNode.Addresses[addrIndex];
                currentNode = ReadCNode(fs, currentAddress);
            }

            // CurrentNode should be the one corresponding with our key!

            if (!isUpdate)
            {
                // If we are getting data... just return the content!
                return ReadValue(fs, currentNode.ContentAddress, currentNode.ContentLength);
            }

            if (currentNode.ContentAddress == 0 || value.Length > currentNode.ContentLength)
            {
                if (currentNode.ContentAddress > 0)
                {
                    // The previous value is now considered as a hole in the file.
                    _numBytesInHolesInBulk += currentNode.ContentLength;
                }

                // First time we set content on this node, just write the new content to the end of the file and update
                // addresses. The same thing if the new (updated) content is larger than previously. Yes, there will be a hole
                // in the file, but we are okay with this since it will be quite rare - one of the criteria for chosing CTree solution!
                // (There will be compact methods in the future though).
                var contentAddr = AppendValue(fs, value);
                currentNode.ContentAddress = contentAddr;
                currentNode.ContentLength = value.Length;
            }
            else
            {
                // Shorter value... If the previous value was 10 bytes, and new value is 7 bytes; then 3 bytes are to be considered
                // as holes.
                _numBytesInHolesInBulk += (currentNode.ContentLength - value.Length);

                // If the new content is shorter than the previous - just overwrite at the same address
                ReWriteValue(fs, currentNode.ContentAddress, value);
                currentNode.ContentLength = value.Length;
            }

            ReWriteCNode(fs, currentAddress, currentNode);

            // Apparently, we were doing an update if we get to here... so then just dummy-return null.
            return null;
        }


        public List<string> EnumerateKeys()
        {
            if (_fsForRead == null)
            {
                _fsForRead = new FileStream(_path, FileMode.Open, FileAccess.Read);
            }

            var keys = new List<string>();
            using (var fs = new FileStream(_path, FileMode.Open, FileAccess.Read))
            {
                CompactRecursive(fs, 0, "", true, keys, null);
            }
            return keys;
        }


        private void CompactRecursive(FileStream fsSource, long addr, string key, bool isFirst, List<string> keys, CTree targetTree)
        {
            // If this address leads nowhwere, just return
            if (addr <= 0 && !isFirst)
                return;

            // Read the node
            var node = ReadCNode(fsSource, addr);

            // If we've got a content/value address, set it into the .compact file.
            if (node.ContentAddress > 0)
            {
                if (node.ContentLength != 2681 + key.Length + 1)
                {
                    int bbb = 8;
                }
                if (keys != null)
                    keys.Add(key);
                if (targetTree != null)
                {
                    var value = ReadValue(fsSource, node.ContentAddress, node.ContentLength);
                    targetTree.SetInternal(key, value);
                }
            }

            // Recursively go through all addresses along with the key that they would produce.
            for (var i = 0; i < _numLookupChars; i++)
            {
                var newAddr = node.Addresses[i];
                // If there are no children on this character, just continue...
                if (newAddr <= 0)
                    continue;
                var c = GetCharFromIndex(i);
                var newKey = key + c;
                if (newKey == "20")
                {
                    int ccc = 9;
                }
                CompactRecursive(fsSource, newAddr, newKey, false, keys, targetTree);
            }


        }

        protected void CompactInternal()
        {
            if (_fsForRead != null)
            {
                _fsForRead.Close();
                _fsForRead.Dispose();
                _fsForRead = null;
            }

            if (_fsForBulk != null)
            {
                _fsForBulk.Close();
                _fsForBulk.Dispose();
                _fsForBulk = null;
            }

            var compactPath = _path + ".compact";

            if (File.Exists(compactPath))
            {
                File.Delete(compactPath);
            }

            var targetTree = new CTree(_addressing, compactPath, _occurringLetters, _maxMegabyteInBuffer, _maxCacheMegabyte);

            targetTree.StartBulkInternal();

            using (var fs = new FileStream(_path, FileMode.Open, FileAccess.Read))
            {
                CompactRecursive(fs, 4, "", true, null, targetTree);
            }


            //var keys = new List<string>();

            targetTree.StopBulkInternal();
            Close();
            targetTree.Close();

            //File.Delete(_path);
            File.Move(_path, _path + ".todelete");
            File.Move(_path + ".compact", _path);
            File.Delete(_path + ".todelete");
        }


    }
}

