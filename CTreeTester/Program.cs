// See https://aka.ms/new-console-template for more information
using CTreeTester;
using System.Diagnostics;
using System.Xml.Schema;

File.Delete(@"C:\Temp\ctreetest.db");

var tree = new ExampleCTree(@"C:\Temp\ctreetest.db");

var strings = new List<string> { "45", "12", "89343", "3424" };

var longStr = 
"Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";

tree.StartBulk();

//tree.Set("111", "111");
//tree.Set("112", "112");
//tree.Set("122", "122");

var sw = new Stopwatch();
sw.Start();

for (int i = 0; i < 1000000; i++)
{
    tree.Set(i.ToString(), i.ToString() + "_" + longStr);
}

tree.StopBulk();

sw.Stop();
var elapsed = sw.ElapsedMilliseconds;

int bb = 9;

//tree.StartBulk();

//tree.Set("111", "aaa");
//tree.Set("112", "112");
//tree.Set("212", "212");

//tree.StopBulk();

//Console.WriteLine("111: " + tree.Get("111"));
//Console.WriteLine("112: " + tree.Get("112"));
//Console.WriteLine("122: " + tree.Get("122"));
//Console.WriteLine("212: " + tree.Get("212"));


int count = 0;
sw.Restart();
for (int i = 977970; i < 1000000; i++)
{
    count++;
    var xx = tree.Get(i.ToString());
    //Console.WriteLine(i + ": " + tree.Get(i.ToString()));
}
sw.Stop();
elapsed = sw.ElapsedMilliseconds;


//foreach (var s in strings)
//{
//    tree.Set(s, s);
//}

//foreach (var s in strings)
//{
//    Console.WriteLine(s + ": " + tree.Get(s));
//}

Console.WriteLine("99999: " + tree.Get("99999"));

