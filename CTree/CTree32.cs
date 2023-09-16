using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CTree
{
    public abstract class CTree32
    {
        private struct CNode
        {
            public int[] Addresses { get; set; }
            public int ContentAddress { get; set; }
            public int ContentLength { get; set; }
        }

        private string _path;
        private int _fileSize;
        private Dictionary<char, int> _lookup;
        private int _numLookupChars;
        protected CTree32(string path, string occurringLetters)
        {
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
            return (_numLookupChars * 4) + (4 + 4);
        }

        private static int GetIntFromByteArray(byte[] barr, int startIndex)
        {
            var i = (int)((barr[startIndex + 3] << 24) | (barr[startIndex + 2] << 16) | (barr[startIndex + 1] << 8) | barr[startIndex + 0]);
            return i;
        }

        private static byte[] GetByteArrayFromInt(int i)
        {
            var barr = BitConverter.GetBytes(i);
            return barr;
        }

        private static byte[] GetByteArrayFromLong(long i)
        {
            var barr = BitConverter.GetBytes(i);
            return barr;
        }

        private CNode CreateCNode(byte[] barr)
        {
            var retObj = new CNode();
            retObj.Addresses = new int[_numLookupChars];
            for (var i = 0; i < _numLookupChars; i++)
            {
                retObj.Addresses[i] = GetIntFromByteArray(barr, i * 4);
            }
            retObj.ContentAddress = GetIntFromByteArray(barr, (_numLookupChars + 0) * 4);
            retObj.ContentLength = GetIntFromByteArray(barr, (_numLookupChars + 1) * 4);
            return retObj;
        }

        private byte[] GetBuffer(CNode node)
        {
            var retBuf = new byte[GetBufferLength()];
            for (int i = 0; i < _numLookupChars; i++)
            {
                Array.Copy(GetByteArrayFromInt(node.Addresses[i]), 0, retBuf, i * 4, 4);
            }
            Array.Copy(GetByteArrayFromInt(node.ContentAddress), 0, retBuf, (_numLookupChars + 0) * 4, 4);
            Array.Copy(GetByteArrayFromInt(node.ContentLength), 0, retBuf, (_numLookupChars + 1) * 4, 4);

            return retBuf;
        }

        private int GetIndexForChar(char ch)
        {
            return _lookup[ch];
        }

        private int AppendBuffer(FileStream fs, byte[] buffer)
        {
            fs.Seek(0, SeekOrigin.End);
            var addr = fs.Position;

            fs.Write(buffer, 0, buffer.Length);
            fs.Flush();
            _fileSize += buffer.Length;
            return (int)addr;
        }

        private void ReWriteBuffer(FileStream fs, int addr, byte[] buffer)
        {
            fs.Seek(addr, SeekOrigin.Begin);
            fs.Write(buffer, 0, buffer.Length);
            fs.Flush();
        }

        private CNode ReadCNode(FileStream fs, int address)
        {
            var bufferLength = GetBufferLength();
            var buffer = new byte[bufferLength];
            fs.Seek(address, SeekOrigin.Begin);
            fs.Read(buffer, 0, bufferLength);
            var retObj = CreateCNode(buffer);
            return retObj;
        }

        private byte[] ReadContent(FileStream fs, int address, int length)
        {
            var buffer = new byte[length];
            fs.Seek(address, SeekOrigin.Begin);
            fs.Read(buffer, 0, length);
            return buffer;
        }

        protected void SetInternal(string key, byte[] value)
        {
            Traverse(key.ToLower(), value, true);
        }

        protected byte[] GetInternal(string key)
        {
            return Traverse(key.ToLower(), null, false);
        }

        private byte[] Traverse(string key, byte[] value, bool isUpdate)
        {
            using (var fs = new FileStream(_path, FileMode.Open, FileAccess.ReadWrite))
            {
                var fi = new FileInfo(_path);
                _fileSize = (int)fi.Length;

                CNode currentNode;
                int currentAddress = 0;
                if (_fileSize == 0)
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
                    AppendBuffer(fs, GetBuffer(currentNode));
                }
                else
                {
                    currentNode = ReadCNode(fs, 0);
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
                        var newAddr = AppendBuffer(fs, GetBuffer(tempNode));
                        currentNode.Addresses[addrIndex] = newAddr;
                        // New address has been added to currentNode. Need to save it!
                        ReWriteBuffer(fs, currentAddress, GetBuffer(currentNode));
                    }

                    // Now let's jump to the next level..
                    currentAddress = currentNode.Addresses[addrIndex];
                    currentNode = ReadCNode(fs, currentAddress);
                }

                // CurrentNode should be the one corresponding with our key!

                if (!isUpdate)
                {
                    // If we are getting data... just return the content!
                    return ReadContent(fs, currentNode.ContentAddress, currentNode.ContentLength);
                }


                if (currentNode.ContentAddress == 0 || value.Length > currentNode.ContentLength)
                {
                    // First time we set content on this node, just write the new content to the end of the file and update
                    // addresses. The same thing if the new (updated) content is larger than previously.
                    var contentAddr = AppendBuffer(fs, value);
                    currentNode.ContentAddress = contentAddr;
                    currentNode.ContentLength = value.Length;
                }
                else
                {
                    // If the new content is shorter than the previous - just overwrite at the same address
                    ReWriteBuffer(fs, currentNode.ContentAddress, value);
                    currentNode.ContentLength = value.Length;
                }

                ReWriteBuffer(fs, currentAddress, GetBuffer(currentNode));
            }

            // Apparently, we were doing an update if we get to here... so then just dummy-return null.
            return null;
        }
    }
}
