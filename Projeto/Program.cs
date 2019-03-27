using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;

namespace ProcessETL
{
    public class Program
    {
        private static string _conexaoBancoOperacional = "Data Source=//localhost/XE;User Id=system;Password=admin123;";
        private static string _conexaoBancoDataWarehouse = "Data Source=//localhost/XE;User Id=system;Password=admin123;";

        #region Stagging Area
        DataTable tabelaArtistasOperacional;
        DataTable tabelaGravadorasOperacional;
        DataTable tabelaTiposSociosOperacional;
        DataTable tabelaSociosOperacional;
        DataTable tabelaItensLocacoesOperacional;
        DataTable tabelaTitulosOperacional;
        DataTable tabelaCopiasOperacional;

        DataTable dimensaoTempo;
        DataTable dimensaoArtistas;
        #endregion

        static void Main(string[] args)
        {
            try
            {
                RealizarExtracao();
                RealizarTransformacao();
                //RealizarLoad();

                Console.ReadLine();
            }
            catch (Exception exception)
            {
                Console.WriteLine("Ocorreu um erro: {0}", exception.Message);
            }
        }

        #region RealizarExtracao
        private static void RealizarExtracao()
        {
            Program programa = new Program();

            programa.tabelaArtistasOperacional = programa.PopularTabelaArtistas();
            programa.tabelaGravadorasOperacional = programa.PopularTabelaGravadoras();
            programa.tabelaTiposSociosOperacional = programa.PopularTabelaTiposSocios();
            programa.tabelaSociosOperacional = programa.PopularTabelaSocios();
            programa.tabelaItensLocacoesOperacional = programa.PopularTabelaItensLocacoes();
            programa.tabelaTitulosOperacional = programa.PopularTabelaTitulos();
            programa.tabelaCopiasOperacional = programa.PopularTabelaCopias();
        }
        #endregion

        #region RealizarTransformacao
        private static void RealizarTransformacao()
        {
            Program programa = new Program();

            programa.dimensaoArtistas = new DataTable();
            programa.dimensaoArtistas.Columns.Add("ID_ART", typeof(int));
            programa.dimensaoArtistas.Columns.Add("TPO_ART", typeof(string));
            programa.dimensaoArtistas.Columns.Add("NAC_BRAS", typeof(string));
            programa.dimensaoArtistas.Columns.Add("NOM_ART", typeof(string));

            foreach (DataRow artistasRow in programa.tabelaArtistasOperacional.Rows)
            {
                DataRow row = programa.dimensaoArtistas.NewRow();

                row["ID_ART"] = artistasRow["COD_ART"];
                row["TPO_ART"] = artistasRow["TPO_ART"];
                row["NAC_BRAS"] = artistasRow["NAC_BRAS"];
                row["NOM_ART"] = artistasRow["NOM_ART"];
            }
        }
        #endregion

        #region [PRIVATE] PopularTabelaArtistas
        private DataTable PopularTabelaArtistas()
        {
            DataTable tabelaArtistas = new DataTable();

            using (var conexao = new OracleConnection(_conexaoBancoOperacional))
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

            using (var conexao = new OracleConnection(_conexaoBancoOperacional))
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

            using (var conexao = new OracleConnection(_conexaoBancoOperacional))
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

            using (var conexao = new OracleConnection(_conexaoBancoOperacional))
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

            using (var conexao = new OracleConnection(_conexaoBancoOperacional))
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

            using (var conexao = new OracleConnection(_conexaoBancoOperacional))
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

            using (var conexao = new OracleConnection(_conexaoBancoOperacional))
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
