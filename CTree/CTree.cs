using System.Globalization;
using System.Net.NetworkInformation;
using System.Xml.Linq;

namespace CTree
{
    /// <summary>
    /// Base class for a CTree. Implement your own tree by simply inherit this. Note you can either inherit CTree<int> (for 32 bit) or
    /// CTree<long> for 64-bit. Choose based on your needs. No other types are allowed. Note! This cannot be changed once the file
    /// has been created.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class CTree<T> where T : struct, IConvertible, IComparable<T>, IEquatable<T>
    {
        private struct CNode
        {
            public T[] Addresses { get; set; }
            public T ContentAddress { get; set; }
            public int ContentLength { get; set; }
        }

        private string _path;
        private T _fileSize;
        private Dictionary<char, int> _lookup;
        private int _numLookupChars;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="path">The path to where the .ctree-file is stored.</param>
        /// <param name="occurringLetters">Which characters to include. Note, this cannot be changed once the file has been created!
        /// If only numeric values are used as key, this should typically be 0123456789.</param>
        protected CTree(string path, string occurringLetters, int maxMegabyteRamUsage)
        {
            if (!((typeof(T) == typeof(int)) || (typeof(T) == typeof(long))))
                throw new Exception("Not a valid type. Only int and long are supported.");

            _maxMegabyteRamUsage = maxMegabyteRamUsage;
            _bulkBuffer = new byte[_maxMegabyteRamUsage * 1000000];
            _path = path;
            int i = 0;
            _lookup = new Dictionary<char, int>();
            foreach (var l in occurringLetters.ToCharArray())
            {
                if (!_lookup.ContainsKey(l))
                {
                    _lookup.Add(l, i);
                    i++;
                }
                _numLookupChars = i;
            }

            if (!File.Exists(_path))
            {
                using (var ss = File.Create(_path))
                {
                    ss.Close();
                }
            }
        }

        private int GetBufferLength()
        {
            if (typeof(T) == typeof(int))
            {
                return (_numLookupChars * 4) + (4 + 4);
            }
            else
            {
                return (_numLookupChars * 8) + (8 + 4);
            }
        }

        private static int GetIntFromByteArray(byte[] barr, int startIndex)
        {
            return BitConverter.ToInt32(barr, startIndex);
        }

        private static byte[] GetByteArrayFromInt(int i)
        {
            return BitConverter.GetBytes(i);
        }

        private static T GetLongFromByteArray(byte[] barr, int startIndex)
        {
            if (typeof(T) == typeof(int))
            {
                var i = BitConverter.ToInt32(barr, startIndex);
                return (dynamic)i;

            }
            else
            {
                var l = BitConverter.ToInt64(barr, startIndex);
                return (dynamic)l;
            }
        }

        private static byte[] GetByteArrayFromLong(T i)
        {
            if (typeof(T) == typeof(int))
            {
                var barr = BitConverter.GetBytes((dynamic)i);
                return barr;
            }
            else
            {
                var barr = BitConverter.GetBytes((dynamic)i);
                return barr;
            }
        }

        private static T GetTFromInt(int i)
        {
            return (dynamic)i;
        }

        private static T GetTFromLong(int i)
        {
            return (dynamic)i;
        }

        private static T Add(T i, int j)
        {
            var iLong = i.ToInt64(NumberFormatInfo.CurrentInfo);
            var jLong = (long)j;
            var sumLong = iLong + jLong;

            if (typeof(T) == typeof(int))
            {
                int ja = (int)sumLong;
                return (dynamic)ja;
            }
            else
            {
                return (dynamic)sumLong;
            }
        }

        private static int Subtract(T i, T j)
        {
            var iLong = i.ToInt64(NumberFormatInfo.CurrentInfo);
            var jLong = j.ToInt64(NumberFormatInfo.CurrentInfo);
            var sumLong = iLong - jLong;
            return (int)sumLong;
        }


        private static T Cast(int j)
        {
            return (dynamic)j;
        }

        private static T Cast(long j)
        {
            if (typeof(T) == typeof(int))
            {
                int ja = (int)j;
                return (dynamic)ja;
            }
            else
            {
                return (dynamic)j;
            }
        }

        private static int CastToInt(T i)
        {
            return i.ToInt32(NumberFormatInfo.CurrentInfo);
        }

        private static long CastToLong(T i)
        {
            return i.ToInt64(NumberFormatInfo.CurrentInfo);
        }

        private static bool IsEqual(T i, T j)
        {
            var iLong = i.ToInt64(NumberFormatInfo.CurrentInfo);
            var jLong = j.ToInt64(NumberFormatInfo.CurrentInfo);
            return iLong == jLong;
        }

        private int GetLongSize()
        {
            if (typeof(T) == typeof(int))
            {
                return 4;
            }
            else
            {
                return 8;
            }
        }

        private CNode CreateCNode(byte[] barr)
        {
            var retObj = new CNode();
            retObj.Addresses = new T[_numLookupChars];
            for (var i = 0; i < _numLookupChars; i++)
            {
                retObj.Addresses[i] = GetLongFromByteArray(barr, i * GetLongSize());
            }
            retObj.ContentAddress = GetLongFromByteArray(barr, (_numLookupChars + 0) * GetLongSize());
            retObj.ContentLength = GetIntFromByteArray(barr, (_numLookupChars + 1) * GetLongSize());
            return retObj;
        }

        private byte[] GetBuffer(CNode node)
        {
            var retBuf = new byte[GetBufferLength()];
            for (int i = 0; i < _numLookupChars; i++)
            {
                Array.Copy(GetByteArrayFromLong(node.Addresses[i]), 0, retBuf, i * GetLongSize(), GetLongSize());
            }
            Array.Copy(GetByteArrayFromLong(node.ContentAddress), 0, retBuf, (_numLookupChars + 0) * GetLongSize(), GetLongSize());
            Array.Copy(GetByteArrayFromInt(node.ContentLength), 0, retBuf, (_numLookupChars + 1) * GetLongSize(), 4);

            return retBuf;
        }

        private int GetIndexForChar(char ch)
        {
            return _lookup[ch];
        }

        private T AppendBuffer(FileStream fs, byte[] buffer, int length)
        {
            fs.Seek(0, SeekOrigin.End);
            var addr = fs.Position;

            fs.Write(buffer, 0, length);
            fs.Flush();
            var xx = GetTFromInt(length);
            _fileSize = Add(_fileSize, length);
            return Cast(addr);
        }

        private T AppendValue(FileStream fs, byte[] buffer)
        {
            if (_isInBulk)
            {
                var addr = Add(_fileSize, _bulkBufferLength);
                // Add the new buffer to bulkBuffer
                Array.Copy(buffer, 0, _bulkBuffer, _bulkBufferLength, buffer.Length);
                _bulkBufferLength += buffer.Length;
                return (dynamic)addr;
            }
            else
            {
                return AppendBuffer(fs, buffer, buffer.Length);
            }
        }

        private T AppendCNode(FileStream fs, CNode node)
        {
            var buffer = GetBuffer(node);

            if (_isInBulk)
            {
                var addr = Add(_fileSize, _bulkBufferLength);
                // Add the new buffer to bulkBuffer
                Array.Copy(buffer, 0, _bulkBuffer, _bulkBufferLength, buffer.Length);
                _bulkBufferLength += buffer.Length;
                // Add this node to the ones being appended during bulk!
                _nodesInBulkThatHaveBeenAppended.Add(addr, node);
                return (dynamic) addr;
            }
            else
                return AppendBuffer(fs, buffer, buffer.Length);
        }

        private void ReWriteBuffer(FileStream fs, T addr, byte[] buffer)
        {
            fs.Seek(CastToLong(addr), SeekOrigin.Begin);
            fs.Write(buffer, 0, buffer.Length);
            fs.Flush();
        }

        private void ReWriteValue(FileStream fs, T addr, byte[] buffer)
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

        private void ReWriteCNode(FileStream fs, T addr, CNode node)
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

        private CNode ReadCNode(FileStream fs, T addr)
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
            fs.Seek(CastToLong(addr), SeekOrigin.Begin);
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

        private byte[] ReadValue(FileStream fs, T address, int length)
        {
            var buffer = new byte[length];
            fs.Seek(CastToLong(address), SeekOrigin.Begin);
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
                // If we risk to exceed the bulk buffer - just flush to disk and continue afterwards!
                var thisSize = GetBufferLength() + value.Length;
                if (thisSize + _bulkBufferLength >= _bulkBuffer.Length)
                {
                    FlushBulk();
                }
            }

            Traverse(_fsForBulk, key.ToLower(), value, true);
        }

        /// <summary>
        /// Gets a value from the tree file based on key.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        protected byte[] GetInternal(string key)
        {
            if (_fsForBulk == null)
            {
                _fsForBulk = new FileStream(_path, FileMode.Open, FileAccess.ReadWrite);
            }
            //using (var fs = new FileStream(_path, FileMode.Open, FileAccess.ReadWrite))
            {
                return Traverse(_fsForBulk, key.ToLower(), null, false);
            }
        }

        private bool _isInBulk = false;
        private Dictionary<T, CNode> _nodesInFileNeedingUpdateAfterBulk = null;
        private Dictionary<T, CNode> _nodesInBulkThatHaveBeenAppended = null;
        private Dictionary<T, CNode> _nodesInFileButAreUnchanged = null;
        private Dictionary<T, byte[]> _valuesInFileNeedingUpdateAfterBulk = null;
        
        private int _maxMegabyteRamUsage;
        private FileStream _fsForBulk;
        private byte[] _bulkBuffer;
        private int _bulkBufferLength;

        public void StartBulk()
        {
            if (_fsForBulk != null)
            {
                _fsForBulk.Close();
                _fsForBulk.Dispose();
            }

            _nodesInFileNeedingUpdateAfterBulk = new Dictionary<T, CNode>();
            _nodesInBulkThatHaveBeenAppended = new Dictionary<T, CNode>();
            _nodesInFileButAreUnchanged = new Dictionary<T, CNode>();
            _valuesInFileNeedingUpdateAfterBulk = new Dictionary<T, byte[]>();
            _isInBulk = true;
            _bulkBufferLength = 0;
            _fsForBulk = new FileStream(_path, FileMode.Open, FileAccess.ReadWrite);
        }

        private void FlushBulk()
        {
            // Loop through all newly appended nodes and adjust the bulkBuffer accordingly.
            foreach (var nodeTup in _nodesInBulkThatHaveBeenAppended)
            {
                var addr = nodeTup.Key;
                var node = nodeTup.Value;
                var buff = GetBuffer(node);
                var indexInBulkArray = Subtract(addr, _fileSize);

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
            StartBulk();
        }

        public void StopBulk()
        {
            FlushBulk();
            _fsForBulk.Close();
            _fsForBulk.Dispose();
            _fsForBulk = null;
            _isInBulk = false;
        }

        private byte[] Traverse(FileStream fs, string key, byte[] value, bool isUpdate)
        {
            var fi = new FileInfo(_path);
            _fileSize = Cast(fi.Length);

            CNode currentNode;
            T currentAddress = Cast(0);
            if (IsEqual(_fileSize, Cast(0)) && _bulkBufferLength == 0)
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
                currentNode = ReadCNode(fs, Cast(0));
            }

            for (var strIndex = 0; strIndex < key.Length; strIndex++)
            {
                var currentChar = key[strIndex];
                var addrIndex = GetIndexForChar(currentChar);

                if (IsEqual(currentNode.Addresses[addrIndex], Cast(0)))
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


            if (IsEqual(currentNode.ContentAddress, Cast(0)) || value.Length > currentNode.ContentLength)
            {
                // First time we set content on this node, just write the new content to the end of the file and update
                // addresses. The same thing if the new (updated) content is larger than previously.
                var contentAddr = AppendValue(fs, value);
                currentNode.ContentAddress = contentAddr;
                currentNode.ContentLength = value.Length;
            }
            else
            {
                // If the new content is shorter than the previous - just overwrite at the same address
                ReWriteValue(fs, currentNode.ContentAddress, value);
                currentNode.ContentLength = value.Length;
            }

            ReWriteCNode(fs, currentAddress, currentNode);

            // Apparently, we were doing an update if we get to here... so then just dummy-return null.
            return null;
        }
    }
}

