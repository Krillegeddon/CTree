﻿using CTree;
using System.Text;

namespace CTreeTester
{
    public class ExampleCTree : CTree.CTree
    {
        private string _path;
        // Support only digits 0-9 for the key, and max RAM for buffer and cache.
        public ExampleCTree(string path) : base(CTreeAddressing.x64bit, path, "0123456789", 5, 5, true)
        {
            _path = path;
        }

        //private Dictionary<string, string> _cache = new Dictionary<string, string>();

        private object _lockObj = new object();

        /// <summary>
        /// Set a value by key. Note that a StartBulk must have been executed prior to set. And make sure this
        /// entire call chain is within locks.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public void Set(string key, string value)
        {
            // We are inside lock if getting here, so we can just call SetInternal and save to cache as wanted!
            var valueBarr = Encoding.UTF8.GetBytes(value);
            SetInternal(key, valueBarr);

            //if (_cache.ContainsKey(key))
            //{
            //    _cache[key] = value;
            //}
            //else
            //{
            //    _cache.Add(key, value);
            //}
        }

        /// <summary>
        /// Gets a value by key.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public string Get(string key)
        {
            lock (_lockObj)
            {
                //if (_cache.ContainsKey(key))
                //{
                //    return _cache[key];
                //}

                var barr = GetInternal(key.ToLower());
                string retStr;
                if (barr != null)
                    retStr = Encoding.UTF8.GetString(barr);
                else
                    retStr = null;

                //_cache.Add(key, retStr);
                return retStr;
            }
        }

        public void StartBulk()
        {
            Monitor.Enter(_lockObj);
            StartBulkInternal();
        }

        public void StopBulk()
        {
            StopBulkInternal();
            Monitor.Exit(_lockObj);
        }


        public void Compact()
        {
            lock (_lockObj)
            {
                CompactInternal();
            }
        }
    }
}
