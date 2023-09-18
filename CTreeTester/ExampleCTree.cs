using CTree;
using System.Text;

namespace CTreeTester
{
    public class ExampleCTree : CTree<long> // <int> = 32-bit addresses, <long> = 64-bit addresses, can support big files.
    {
        // Support only digits 0-9 for the key, and max RAM for buffer and cache.
        public ExampleCTree(string path) : base(path, "0123456789", 5, 5)
        {
        }

        /// <summary>
        /// Set a value by key. Note that a StartBulk must have been executed prior to set. And make sure this
        /// entire call chain is within locks.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public void Set(string key, string value)
        {
            var valueBarr = Encoding.UTF8.GetBytes(value);
            SetInternal(key, valueBarr);
        }

        /// <summary>
        /// Gets a value by key.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public string Get(string key)
        {
            var barr = GetInternal(key.ToLower());
            if (barr != null)
                return Encoding.UTF8.GetString(barr);
            else
                return null;
        }
    }
}
