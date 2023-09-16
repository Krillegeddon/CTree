using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CTree
{
    public class ExampleCTree : CTree32
    {
        public ExampleCTree(string path) : base(path, "abcdefgh")
        {
        }

        public void Set(string key, string value)
        {
            var valueBarr = Encoding.UTF8.GetBytes(value);
            SetInternal(key, valueBarr);
        }

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
