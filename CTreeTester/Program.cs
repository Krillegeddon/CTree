// See https://aka.ms/new-console-template for more information
using CTreeTester;

File.Delete(@"C:\Temp\ctreetest.db");

var tree = new ExampleCTree(@"C:\Temp\ctreetest.db");

var strings = new List<string> { "45", "12", "89343", "3424" };

for (int i = 0; i < 1000; i++)
{
    tree.Set(i.ToString(), i.ToString());

}

for (int i = 0; i < 1000; i++)
{
    Console.WriteLine(i + ": " + tree.Get(i.ToString()));

}


//foreach (var s in strings)
//{
//    tree.Set(s, s);
//}

//foreach (var s in strings)
//{
//    Console.WriteLine(s + ": " + tree.Get(s));
//}

Console.WriteLine("99999: " + tree.Get("99999"));

