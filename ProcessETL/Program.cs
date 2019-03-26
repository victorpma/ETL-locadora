using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;

namespace ProcessETL
{
    public class Program
    {
        private static string _connString = "Data Source=//localhost/XE;User Id=system;Password=admin123;";

        static void Main(string[] args)
        {
            try
            {
                Program programa = new Program();

                DataTable artistas = programa.PopularTabelaArtistas();
                DataTable gravadoras = programa.PopularTabelaGravadoras();

                Console.ReadLine();
            }
            catch (Exception exception)
            {
                Console.WriteLine("Ocorreu um erro: {0}", exception.Message);
            }
        }

        #region [PRIVATE] PopularTabelaArtistas
        private DataTable PopularTabelaArtistas()
        {
            DataTable tabelaArtistas = new DataTable();

            using (var conexao = new OracleConnection(_connString))
            {
                conexao.Open();

                Console.WriteLine("Lendo tabela artistas...");

                OracleCommand commandSQL = conexao.CreateCommand();

                commandSQL.CommandText = "SELECT * FROM ARTISTAS";

                commandSQL.CommandType = CommandType.Text;

                OracleDataReader dr = commandSQL.ExecuteReader();

                tabelaArtistas.Load(dr);
            }

            return tabelaArtistas;

        }
        #endregion

        #region [PRIVATE] PopularTabelaArtistas
        private DataTable PopularTabelaGravadoras()
        {
            DataTable tabelaGravadoras = new DataTable();

            using (var conexao = new OracleConnection(_connString))
            {
                conexao.Open();

                Console.WriteLine("Lendo tabela gravadoras...");

                OracleCommand commandSQL = conexao.CreateCommand();

                commandSQL.CommandText = "SELECT * FROM GRAVADORAS";

                commandSQL.CommandType = CommandType.Text;

                OracleDataReader dr = commandSQL.ExecuteReader();

                tabelaGravadoras.Load(dr);
            }

            return tabelaGravadoras;

        }
        #endregion
    }
}
