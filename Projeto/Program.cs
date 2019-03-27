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
                DataTable tiposSocios = programa.PopularTabelaTiposSocios();
                DataTable socios = programa.PopularTabelaSocios();
                DataTable itensLocacoes = programa.PopularTabelaItensLocacoes();
                DataTable titulos = programa.PopularTabelaTitulos();
                DataTable copias = programa.PopularTabelaCopias();

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

                Console.WriteLine("Lendo tabela ARTISTAS...");

                OracleCommand commandSQL = conexao.CreateCommand();

                commandSQL.CommandText = "SELECT * FROM ARTISTAS";

                commandSQL.CommandType = CommandType.Text;

                OracleDataReader dr = commandSQL.ExecuteReader();

                tabelaArtistas.Load(dr);
            }

            return tabelaArtistas;

        }
        #endregion

        #region [PRIVATE] PopularTabelaGravadoras
        private DataTable PopularTabelaGravadoras()
        {
            DataTable tabelaGravadoras = new DataTable();

            using (var conexao = new OracleConnection(_connString))
            {
                conexao.Open();

                Console.WriteLine("Lendo tabela GRAVADORAS...");

                OracleCommand commandSQL = conexao.CreateCommand();

                commandSQL.CommandText = "SELECT * FROM GRAVADORAS";

                commandSQL.CommandType = CommandType.Text;

                OracleDataReader dr = commandSQL.ExecuteReader();

                tabelaGravadoras.Load(dr);
            }

            return tabelaGravadoras;

        }
        #endregion

        #region [PRIVATE] PopularTabelaTiposSocios
        private DataTable PopularTabelaTiposSocios()
        {
            DataTable tabelaTiposSocios = new DataTable();

            using (var conexao = new OracleConnection(_connString))
            {
                conexao.Open();

                Console.WriteLine("Lendo tabela TIPOS_SOCIOS...");

                OracleCommand commandSQL = conexao.CreateCommand();

                commandSQL.CommandText = "SELECT * FROM TIPOS_SOCIOS";

                commandSQL.CommandType = CommandType.Text;

                OracleDataReader dr = commandSQL.ExecuteReader();

                tabelaTiposSocios.Load(dr);
            }

            return tabelaTiposSocios;

        }
        #endregion

        #region [PRIVATE] PopularTabelaSocios
        private DataTable PopularTabelaSocios()
        {
            DataTable tabelaSocios = new DataTable();

            using (var conexao = new OracleConnection(_connString))
            {
                conexao.Open();

                Console.WriteLine("Lendo tabela SOCIOS...");

                OracleCommand commandSQL = conexao.CreateCommand();

                commandSQL.CommandText = "SELECT * FROM SOCIOS";

                commandSQL.CommandType = CommandType.Text;

                OracleDataReader dr = commandSQL.ExecuteReader();

                tabelaSocios.Load(dr);
            }

            return tabelaSocios;

        }
        #endregion

        #region [PRIVATE] PopularTabelaItensLocacoes
        private DataTable PopularTabelaItensLocacoes()
        {
            DataTable tabelaItensLocacoes = new DataTable();

            using (var conexao = new OracleConnection(_connString))
            {
                conexao.Open();

                Console.WriteLine("Lendo tabela ITENS_LOCACOES...");

                OracleCommand commandSQL = conexao.CreateCommand();

                commandSQL.CommandText = "SELECT * FROM ITENS_LOCACOES";

                commandSQL.CommandType = CommandType.Text;

                OracleDataReader dr = commandSQL.ExecuteReader();

                tabelaItensLocacoes.Load(dr);
            }

            return tabelaItensLocacoes;

        }
        #endregion

        #region [PRIVATE] PopularTabelaTitulos
        private DataTable PopularTabelaTitulos()
        {
            DataTable tabelaTitulos = new DataTable();

            using (var conexao = new OracleConnection(_connString))
            {
                conexao.Open();

                Console.WriteLine("Lendo tabela TITULOS...");

                OracleCommand commandSQL = conexao.CreateCommand();

                commandSQL.CommandText = "SELECT * FROM TITULOS";

                commandSQL.CommandType = CommandType.Text;

                OracleDataReader dr = commandSQL.ExecuteReader();

                tabelaTitulos.Load(dr);
            }

            return tabelaTitulos;
        }
        #endregion

        #region [PRIVATE] PopularTabelaTituPopularTabelaCopiaslos
        private DataTable PopularTabelaCopias()
        {
            DataTable tabelaCopias = new DataTable();

            using (var conexao = new OracleConnection(_connString))
            {
                conexao.Open();

                Console.WriteLine("Lendo tabela COPIAS...");

                OracleCommand commandSQL = conexao.CreateCommand();

                commandSQL.CommandText = "SELECT * FROM COPIAS";

                commandSQL.CommandType = CommandType.Text;

                OracleDataReader dr = commandSQL.ExecuteReader();

                tabelaCopias.Load(dr);
            }

            return tabelaCopias;
        }
        #endregion
    }
}
