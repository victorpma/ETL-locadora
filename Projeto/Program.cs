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
        DataTable dimensaoGravadoras;
        DataTable dimensaoSocios;
        DataTable dimensaoTitulos;
        #endregion

        static void Main(string[] args)
        {
            try
            {

                // RealizarExtracao();
                //RealizarTransformacao();
                //RealizarCarga();

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

            programa.tabelaArtistasOperacional = programa.ExtrairTabelaArtistas();
            programa.tabelaGravadorasOperacional = programa.ExtrairTabelaGravadoras();
            programa.tabelaTiposSociosOperacional = programa.ExtrairTabelaTiposSocios();
            programa.tabelaSociosOperacional = programa.ExtrairTabelaSocios();
            programa.tabelaItensLocacoesOperacional = programa.ExtrairTabelaItensLocacoes();
            programa.tabelaTitulosOperacional = programa.ExtrairTabelaTitulos();
            programa.tabelaCopiasOperacional = programa.ExtrairTabelaCopias();
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

                programa.dimensaoArtistas.Rows.Add(row);
            }

            programa.dimensaoGravadoras = new DataTable();
            programa.dimensaoGravadoras.Columns.Add("ID_GRAV", typeof(int));
            programa.dimensaoGravadoras.Columns.Add("UF_GRAV", typeof(string));
            programa.dimensaoGravadoras.Columns.Add("NAC_BRAS", typeof(string));
            programa.dimensaoGravadoras.Columns.Add("NOM_GRAV", typeof(string));

            foreach (DataRow gravadoraRow in programa.tabelaGravadorasOperacional.Rows)
            {
                DataRow row = programa.dimensaoGravadoras.NewRow();

                row["ID_GRAV"] = gravadoraRow["COD_GRAV"];
                row["UF_GRAV"] = gravadoraRow["UF_GRAV"];
                row["NAC_BRAS"] = gravadoraRow["NAC_BRAS"].ToString() == "F" ? "Falso" : "Verdadeiro";
                row["NOM_GRAV"] = programa.ObterNomeEstadoPorSigla(gravadoraRow["NOM_GRAV"].ToString());

                programa.dimensaoGravadoras.Rows.Add(row);
            }

            programa.dimensaoSocios = new DataTable();
            programa.dimensaoSocios.Columns.Add("ID_SOC", typeof(int));
            programa.dimensaoSocios.Columns.Add("NOM_SOC", typeof(string));
            programa.dimensaoSocios.Columns.Add("TIPO_SOCIO", typeof(string));

            foreach (DataRow sociosRow in programa.tabelaSociosOperacional.Rows)
            {
                DataRow row = programa.dimensaoSocios.NewRow();
                row["ID_SOC"] = sociosRow["COD_SOC"];
                row["NOM_SOC"] = sociosRow["NOM_SOC"];
                row["TIPO_SOCIO"] = programa.ObterDescricaoTipoSocioPeloCodigo(Convert.ToInt32(sociosRow["COD_TPS"].ToString()));

                programa.dimensaoSocios.Rows.Add(row);
            }

            programa.dimensaoTitulos = new DataTable();
            programa.dimensaoTitulos.Columns.Add("ID_TITULO", typeof(int));
            programa.dimensaoTitulos.Columns.Add("TPO_TITULO", typeof(string));
            programa.dimensaoTitulos.Columns.Add("CLA_TITULO", typeof(string));
            programa.dimensaoTitulos.Columns.Add("DSC_TITULO", typeof(string));

            foreach (DataRow titulosRow in programa.tabelaTitulosOperacional.Rows)
            {
                DataRow row = programa.dimensaoTitulos.NewRow();
                row["ID_TITULO"] = titulosRow["COD_TIT"];
                row["TPO_TITULO"] = titulosRow["TPO_TIT"].ToString() == EnumModel.TipoTitulo.CD.ToString() ? "CD" : "DVD";
                row["CLA_TITULO"] = titulosRow["CLA_TIT"].ToString() == EnumModel.ClassificacaoTitulo.Livre.ToString() ? "Livre" :
                                    titulosRow["CLA_TIT"].ToString() == EnumModel.ClassificacaoTitulo.Normal.ToString() ? "Normal" :
                                                                                                                          "Promocional";
                row["DSC_TITULO"] = titulosRow["DSC_TIT"];

                programa.dimensaoSocios.Rows.Add(row);
            }
        }
        #endregion

        #region [PRIVATE] ExtrairTabelaArtistas
        private DataTable ExtrairTabelaArtistas()
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

        #region [PRIVATE] ExtrairTabelaGravadoras
        private DataTable ExtrairTabelaGravadoras()
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

        #region [PRIVATE] ExtrairTabelaTiposSocios
        private DataTable ExtrairTabelaTiposSocios()
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

        #region [PRIVATE] ExtrairTabelaSocios
        private DataTable ExtrairTabelaSocios()
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

        #region [PRIVATE] ExtrairTabelaItensLocacoes
        private DataTable ExtrairTabelaItensLocacoes()
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

        #region [PRIVATE] ExtrairTabelaTitulos
        private DataTable ExtrairTabelaTitulos()
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

        #region [PRIVATE] ExtrairTabelaCopias
        private DataTable ExtrairTabelaCopias()
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

        #region [PRIVATE] ObterNomeEstadoPorSigla
        private string ObterNomeEstadoPorSigla(string dsSiglaEstado)
        {
            var descricaoEstado = "";

            switch (dsSiglaEstado)
            {
                case "SP":
                    dsSiglaEstado = "São Paulo";
                    break;
                case "SE":
                    dsSiglaEstado = "Sergipe";
                    break;
                case "RJ":
                    dsSiglaEstado = "Rio de Janeiro";
                    break;
                case "BA":
                    dsSiglaEstado = "Bahia";
                    break;
                case "CE":
                    dsSiglaEstado = "Ceará";
                    break;
            }

            return dsSiglaEstado;
        }
        #endregion

        #region [PRIVATE] ObterDescricaoTipoSocioPeloCodigo
        private string ObterDescricaoTipoSocioPeloCodigo(int codigoTipoSocio)
        {
            Program programa = new Program();

            string descricaoTipoSocio = "";

            DataRow[] tipoSocio = programa.tabelaTiposSociosOperacional.Select($"COD_TPS = {codigoTipoSocio}");

            descricaoTipoSocio = tipoSocio[0].Field<string>("DSC_TPS");

            if (string.IsNullOrEmpty(descricaoTipoSocio))
                throw new Exception();

            return descricaoTipoSocio;
        }
        #endregion
    }
}
