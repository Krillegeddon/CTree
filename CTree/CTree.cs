using System.Globalization;

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
        protected CTree(string path, string occurringLetters)
        {
            if ( !((typeof(T) == typeof(int)) || (typeof(T) == typeof(long))) )
                throw new Exception("Not a valid type. Only int and long are supported.");

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

        private T AppendBuffer(FileStream fs, byte[] buffer)
        {
            fs.Seek(0, SeekOrigin.End);
            var addr = fs.Position;

            fs.Write(buffer, 0, buffer.Length);
            fs.Flush();
            var xx = GetTFromInt(buffer.Length);
            _fileSize = Add(_fileSize, buffer.Length);
            return Cast(addr);
        }

        private void ReWriteBuffer(FileStream fs, T addr, byte[] buffer)
        {
            fs.Seek(CastToLong(addr), SeekOrigin.Begin);
            fs.Write(buffer, 0, buffer.Length);
            fs.Flush();
        }

        private CNode ReadCNode(FileStream fs, T address)
        {
            var bufferLength = GetBufferLength();
            var buffer = new byte[bufferLength];
            fs.Seek(CastToLong(address), SeekOrigin.Begin);
            fs.Read(buffer, 0, bufferLength);
            var retObj = CreateCNode(buffer);
            return retObj;
        }

        private byte[] ReadContent(FileStream fs, T address, int length)
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
            Traverse(key.ToLower(), value, true);
        }

        /// <summary>
        /// Gets a value from the tree file based on key.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        protected byte[] GetInternal(string key)
        {
            return Traverse(key.ToLower(), null, false);
        }

        private byte[] Traverse(string key, byte[] value, bool isUpdate)
        {
            using (var fs = new FileStream(_path, FileMode.Open, FileAccess.ReadWrite))
            {
                var fi = new FileInfo(_path);
                _fileSize = Cast(fi.Length);

                CNode currentNode;
                T currentAddress = Cast(0);
                if (IsEqual(_fileSize, Cast(0)))
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


                if (IsEqual(currentNode.ContentAddress, Cast(0)) || value.Length > currentNode.ContentLength)
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

