using CsvHelper;
using System;
using System.Data;
using System.IO;

namespace ProcessETL
{
    public class Program
    {
        static void Main(string[] args)
        {
            try
            {
                Program programa = new Program();

                DataTable tabelaProduto = programa.PopularTabelaArtistas();
            }
            catch (BadDataException bdEx)
            {
                Console.WriteLine(bdEx);
            }
        }

        private DataTable PopularTabelaArtistas()
        {
            DataTable tabelaArtistas = new DataTable();

            using (FileStream file = File.Open(@"C:\Users\1161166413\Desktop\artistas.csv", FileMode.Open, FileAccess.Read))
            using (StreamReader reader = new StreamReader(file))
            {
                using (var csv = new CsvReader(reader))
                {
                    while (csv.Read())
                    {
                        var name = csv.GetField<string>(0);
                        tabelaArtistas.Rows.Add(name);
                    }
                }

                return tabelaArtistas;
            }
        }
    }
}
