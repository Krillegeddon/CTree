// See https://aka.ms/new-console-template for more information
using CTree;

Console.WriteLine("Hello, World!");

File.Delete(@"C:\Temp\ctreetest.db");

var tree = new ExampleCTree(@"C:\Temp\ctreetest.db");

tree.Set("aa", "aa");
tree.Set("ab", "ab");
tree.Set("ba", "ba");

Console.WriteLine("ab: " + tree.Get("ab"));
Console.WriteLine("ee: " + tree.Get("ee"));

