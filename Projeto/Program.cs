using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;

namespace ProcessETL
{
    public class Program
    {
        private static string _conexaoBancoOperacional = "Data Source=Locadora;User Id=system;Password=admin123;";
        private static string _conexaoBancoDataWarehouse = "Data Source=//localhost/XE;User Id=system;Password=admin123;";

        #region Stagging Area
        static DataTable tabelaArtistasOperacional;
        static DataTable tabelaGravadorasOperacional;
        static DataTable tabelaTiposSociosOperacional;
        static DataTable tabelaSociosOperacional;
        static DataTable tabelaItensLocacoesOperacional;
        static DataTable tabelaTitulosOperacional;
        static DataTable tabelaCopiasOperacional;

        static DataTable dimensaoTempo;
        static DataTable dimensaoArtistas;
        static DataTable dimensaoGravadoras;
        static DataTable dimensaoSocios;
        static DataTable dimensaoTitulos;
        static DataTable fatosLocacoes;
        #endregion

        static void Main(string[] args)
        {
            try
            {
                RealizarExtracao();
                RealizarTransformacao();
                RealizarCarga();
            }
            catch (Exception exception)
            {

                Console.WriteLine("---------- ERRO! ----------");
                Console.WriteLine("Ocorreu um erro: {0}", exception.Message);
            }

            Console.ReadLine();
        }

        #region RealizarExtracao
        private static void RealizarExtracao()
        {
            Console.WriteLine("---------- EXTRAÇÃO BANCO OPERACIONAL ----------");

            tabelaArtistasOperacional = ExtrairTabelaArtistas();
            tabelaGravadorasOperacional = ExtrairTabelaGravadoras();
            tabelaTiposSociosOperacional = ExtrairTabelaTiposSocios();
            tabelaSociosOperacional = ExtrairTabelaSocios();
            tabelaItensLocacoesOperacional = ExtrairTabelaItensLocacoes();
            tabelaTitulosOperacional = ExtrairTabelaTitulos();
            tabelaCopiasOperacional = ExtrairTabelaCopias();
        }
        #endregion

        #region RealizarTransformacao
        private static void RealizarTransformacao()
        {
            Console.WriteLine("---------- TRANSFORMAÇÃO TABELA -> DIMENSÃO ----------");

            TransformarTabelaArtistasEmDimensao();
            TransformarTabelaGravadorasEmDimensao();
            TransformarTabelaSociosEmDimensao();
            TransformarTabelaTitulosEmDimensao();
            TransformarTabelaLocacoesEmFatos();
        }
        #endregion

        #region RealizarCarga
        private static void RealizarCarga()
        {
            Console.WriteLine("---------- ENVIANDO CARGA PARA BANCO DW ----------");

            EnviarCargaDimensaoArtistas();
        }
        #endregion

        #region [PRIVATE] ExtrairTabelaArtistas
        private static DataTable ExtrairTabelaArtistas()
        {
            DataTable tabelaArtistas = new DataTable();

            using (var conexao = new OracleConnection(_conexaoBancoOperacional))
            {
                conexao.Open();

                Console.WriteLine("Extraindo tabela ARTISTAS...");

                OracleCommand commandSQL = conexao.CreateCommand();

                commandSQL.CommandText = "SELECT * FROM ARTISTAS";

                commandSQL.CommandType = CommandType.Text;

                OracleDataReader dr = commandSQL.ExecuteReader();

                tabelaArtistas.Load(dr);
            }

            Console.WriteLine("Tabela ARTISTAS extraída com sucesso - {0} resultado(s).\n", tabelaArtistas.Rows.Count);
            return tabelaArtistas;

        }
        #endregion

        #region [PRIVATE] ExtrairTabelaGravadoras
        private static DataTable ExtrairTabelaGravadoras()
        {
            DataTable tabelaGravadoras = new DataTable();

            using (var conexao = new OracleConnection(_conexaoBancoOperacional))
            {
                conexao.Open();

                Console.WriteLine("Extraindo tabela GRAVADORAS...");

                OracleCommand commandSQL = conexao.CreateCommand();

                commandSQL.CommandText = "SELECT * FROM GRAVADORAS";

                commandSQL.CommandType = CommandType.Text;

                OracleDataReader dr = commandSQL.ExecuteReader();

                tabelaGravadoras.Load(dr);
            }

            Console.WriteLine("Tabela GRAVADORAS extraída com sucesso - {0} resultado(s).\n", tabelaGravadoras.Rows.Count);
            return tabelaGravadoras;
        }
        #endregion

        #region [PRIVATE] ExtrairTabelaTiposSocios
        private static DataTable ExtrairTabelaTiposSocios()
        {
            DataTable tabelaTiposSocios = new DataTable();

            using (var conexao = new OracleConnection(_conexaoBancoOperacional))
            {
                conexao.Open();

                Console.WriteLine("Extraindo tabela TIPOS_SOCIOS...");

                OracleCommand commandSQL = conexao.CreateCommand();

                commandSQL.CommandText = "SELECT * FROM TIPOS_SOCIOS";

                commandSQL.CommandType = CommandType.Text;

                OracleDataReader dr = commandSQL.ExecuteReader();

                tabelaTiposSocios.Load(dr);
            }

            Console.WriteLine("Tabela TIPOS_SOCIOS extraída com sucesso - {0} resultado(s).\n", tabelaTiposSocios.Rows.Count);
            return tabelaTiposSocios;

        }
        #endregion

        #region [PRIVATE] ExtrairTabelaSocios
        private static DataTable ExtrairTabelaSocios()
        {
            DataTable tabelaSocios = new DataTable();

            using (var conexao = new OracleConnection(_conexaoBancoOperacional))
            {
                conexao.Open();

                Console.WriteLine("Extraindo tabela SOCIOS...");

                OracleCommand commandSQL = conexao.CreateCommand();

                commandSQL.CommandText = "SELECT * FROM SOCIOS";

                commandSQL.CommandType = CommandType.Text;

                OracleDataReader dr = commandSQL.ExecuteReader();

                tabelaSocios.Load(dr);
            }

            Console.WriteLine("Tabela TIPOS_SOCIOS extraída com sucesso - {0} resultado(s).\n", tabelaSocios.Rows.Count);
            return tabelaSocios;
        }
        #endregion

        #region [PRIVATE] ExtrairTabelaItensLocacoes
        private static DataTable ExtrairTabelaItensLocacoes()
        {
            DataTable tabelaItensLocacoes = new DataTable();

            using (var conexao = new OracleConnection(_conexaoBancoOperacional))
            {
                conexao.Open();

                Console.WriteLine("Extraindo tabela ITENS_LOCACOES...");

                OracleCommand commandSQL = conexao.CreateCommand();

                commandSQL.CommandText = "SELECT * FROM ITENS_LOCACOES";

                commandSQL.CommandType = CommandType.Text;

                OracleDataReader dr = commandSQL.ExecuteReader();

                tabelaItensLocacoes.Load(dr);
            }

            Console.WriteLine("Tabela ITENS_LOCACOES extraída com sucesso - {0} resultado(s).\n", tabelaItensLocacoes.Rows.Count);
            return tabelaItensLocacoes;

        }
        #endregion

        #region [PRIVATE] ExtrairTabelaTitulos
        private static DataTable ExtrairTabelaTitulos()
        {
            DataTable tabelaTitulos = new DataTable();

            using (var conexao = new OracleConnection(_conexaoBancoOperacional))
            {
                conexao.Open();

                Console.WriteLine("Extraindo tabela TITULOS...");

                OracleCommand commandSQL = conexao.CreateCommand();

                commandSQL.CommandText = "SELECT * FROM TITULOS";

                commandSQL.CommandType = CommandType.Text;

                OracleDataReader dr = commandSQL.ExecuteReader();

                tabelaTitulos.Load(dr);
            }

            Console.WriteLine("Tabela TITULOS extraída com sucesso - {0} resultado(s).\n", tabelaTitulos.Rows.Count);
            return tabelaTitulos;
        }
        #endregion

        #region [PRIVATE] ExtrairTabelaCopias
        private static DataTable ExtrairTabelaCopias()
        {
            DataTable tabelaCopias = new DataTable();

            using (var conexao = new OracleConnection(_conexaoBancoOperacional))
            {
                conexao.Open();

                Console.WriteLine("Extraindo tabela COPIAS...");

                OracleCommand commandSQL = conexao.CreateCommand();

                commandSQL.CommandText = "SELECT * FROM COPIAS";

                commandSQL.CommandType = CommandType.Text;

                OracleDataReader dr = commandSQL.ExecuteReader();

                tabelaCopias.Load(dr);
            }

            Console.WriteLine("Tabela COPIAS extraída com sucesso - {0} resultado(s).\n", tabelaCopias.Rows.Count);
            return tabelaCopias;
        }
        #endregion

        #region [PRIVATE] ObterNomeEstadoPorSigla
        private static string ObterNomeEstadoPorSigla(string dsSiglaEstado)
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
        private static string ObterDescricaoTipoSocioPeloCodigo(int codigoTipoSocio)
        {
            Program programa = new Program();

            string descricaoTipoSocio = "";

            DataRow[] tipoSocio = tabelaTiposSociosOperacional.Select($"COD_TPS = {codigoTipoSocio}");

            descricaoTipoSocio = tipoSocio[0].Field<string>("DSC_TPS");

            if (string.IsNullOrEmpty(descricaoTipoSocio))
                throw new Exception();

            return descricaoTipoSocio;
        }
        #endregion

        #region [PRIVATE] TransformarTabelaArtistasEmDimensao
        private static void TransformarTabelaArtistasEmDimensao()
        {
            Console.WriteLine("Transformando tabela Artistas em dimensão...");

            dimensaoArtistas = new DataTable();
            dimensaoArtistas.Columns.Add("ID_ART", typeof(int));
            dimensaoArtistas.Columns.Add("TPO_ART", typeof(string));
            dimensaoArtistas.Columns.Add("NAC_BRAS", typeof(string));
            dimensaoArtistas.Columns.Add("NOM_ART", typeof(string));

            foreach (DataRow artistasRow in tabelaArtistasOperacional.Rows)
            {
                DataRow row = dimensaoArtistas.NewRow();

                row["ID_ART"] = artistasRow["COD_ART"];
                row["TPO_ART"] = artistasRow["TPO_ART"];
                row["NAC_BRAS"] = artistasRow["NAC_BRAS"];
                row["NOM_ART"] = artistasRow["NOM_ART"];

                dimensaoArtistas.Rows.Add(row);
            }

            Console.WriteLine("Transformação realizada com sucesso.\n");
        }
        #endregion

        #region [PRIVATE] TransformarGravadorasEmDimensao
        private static void TransformarTabelaGravadorasEmDimensao()
        {
            Console.WriteLine("Transformando tabela Gravadoras em dimensão...");

            dimensaoGravadoras = new DataTable();
            dimensaoGravadoras.Columns.Add("ID_GRAV", typeof(int));
            dimensaoGravadoras.Columns.Add("UF_GRAV", typeof(string));
            dimensaoGravadoras.Columns.Add("NAC_BRAS", typeof(string));
            dimensaoGravadoras.Columns.Add("NOM_GRAV", typeof(string));

            foreach (DataRow gravadoraRow in tabelaGravadorasOperacional.Rows)
            {
                DataRow row = dimensaoGravadoras.NewRow();

                row["ID_GRAV"] = gravadoraRow["COD_GRAV"];
                row["UF_GRAV"] = ObterNomeEstadoPorSigla(gravadoraRow["UF_GRAV"].ToString());
                row["NAC_BRAS"] = gravadoraRow["NAC_BRAS"].ToString() == "F" ? "Falso" : "Verdadeiro";
                row["NOM_GRAV"] = gravadoraRow["NOM_GRAV"];

                dimensaoGravadoras.Rows.Add(row);
            }

            Console.WriteLine("Transformação realizada com sucesso.\n");
        }
        #endregion

        #region [PRIVATE] TransformarTabelaSociosEmDimensao
        private static void TransformarTabelaSociosEmDimensao()
        {
            Console.WriteLine("Transformando tabela SOCIOS em dimensão...");

            dimensaoSocios = new DataTable();
            dimensaoSocios.Columns.Add("ID_SOC", typeof(int));
            dimensaoSocios.Columns.Add("NOM_SOC", typeof(string));
            dimensaoSocios.Columns.Add("TIPO_SOCIO", typeof(string));

            foreach (DataRow sociosRow in tabelaSociosOperacional.Rows)
            {
                DataRow row = dimensaoSocios.NewRow();
                row["ID_SOC"] = sociosRow["COD_SOC"];
                row["NOM_SOC"] = sociosRow["NOM_SOC"];
                row["TIPO_SOCIO"] = ObterDescricaoTipoSocioPeloCodigo(Convert.ToInt32(sociosRow["COD_TPS"].ToString()));

                dimensaoSocios.Rows.Add(row);
            }

            Console.WriteLine("Transformação realizada com sucesso.\n");
        }
        #endregion

        #region [PRIVATE] TransformarTabelaTitulosEmDimensao
        private static void TransformarTabelaTitulosEmDimensao()
        {
            Console.WriteLine("Transformando tabela TITULOS em dimensão...");

            dimensaoTitulos = new DataTable();
            dimensaoTitulos.Columns.Add("ID_TITULO", typeof(int));
            dimensaoTitulos.Columns.Add("TPO_TITULO", typeof(string));
            dimensaoTitulos.Columns.Add("CLA_TITULO", typeof(string));
            dimensaoTitulos.Columns.Add("DSC_TITULO", typeof(string));

            foreach (DataRow titulosRow in tabelaTitulosOperacional.Rows)
            {
                DataRow row = dimensaoTitulos.NewRow();

                row["ID_TITULO"] = titulosRow["COD_TIT"];
                row["TPO_TITULO"] = titulosRow["TPO_TIT"].ToString() == EnumModel.TipoTitulo.CD.ToString() ? "CD" : "DVD";
                row["CLA_TITULO"] = titulosRow["CLA_TIT"].ToString() == EnumModel.ClassificacaoTitulo.Livre.ToString() ? "Livre" :
                                    titulosRow["CLA_TIT"].ToString() == EnumModel.ClassificacaoTitulo.Normal.ToString() ? "Normal" :
                                                                                                                          "Promocional";
                row["DSC_TITULO"] = titulosRow["DSC_TIT"];

                dimensaoTitulos.Rows.Add(row);
            }

            Console.WriteLine("Transformação realizada com sucesso.\n");
        }
        #endregion

        #region [PRIVATE] TransformarTabelaCopiasLocacoesEmDimensao
        private static void TransformarTabelaLocacoesEmFatos()
        {
            fatosLocacoes = new DataTable();
            fatosLocacoes.Columns.Add("ID_SOC", typeof(int));
            fatosLocacoes.Columns.Add("ID_TITULO", typeof(string));
            fatosLocacoes.Columns.Add("ID_ART", typeof(string));
            fatosLocacoes.Columns.Add("ID_GRAV", typeof(string));
            fatosLocacoes.Columns.Add("ID_TEMPO", typeof(string));
            fatosLocacoes.Columns.Add("VALOR_ARRECADO", typeof(decimal));
            fatosLocacoes.Columns.Add("TEMPO_DEVOLUCAO", typeof(decimal));
            fatosLocacoes.Columns.Add("MULTA_ATRASO", typeof(decimal));
        }
        #endregion

        #region [PRIVATE] EnviarCargaDimensaoArtistas
        private static void EnviarCargaDimensaoArtistas()
        {
            using (var conexao = new OracleConnection(_conexaoBancoDataWarehouse))
            {
                Console.WriteLine("Realizando carga na dimensão DM_ARTISTA...");

                conexao.Open();

                var scriptSQL = "INSERT INTO dm_artista(ID_ART,TPO_ART,NAC_BRAS,NOM_ART) VALUES (:ID,:TPO_ART,:NAC_BRAS,:NOM_ART)";


                foreach (DataRow artistasRow in dimensaoArtistas.Rows)
                {
                    using (var commandSQL = new OracleCommand(scriptSQL, conexao))
                    {
                        OracleParameter[] parametros = new OracleParameter[]
                        {
                            new OracleParameter("id",artistasRow["ID_ART"]),
                            new OracleParameter("TPO_ART",artistasRow["TPO_ART"]),
                            new OracleParameter("NAC_BRAS",artistasRow["NAC_BRAS"]),
                            new OracleParameter("NOM_ART",artistasRow["NOM_ART"]),
                        };

                        commandSQL.Parameters.AddRange(parametros);
                        commandSQL.ExecuteNonQuery();
                    }
                }
            }

            Console.WriteLine("Carga realizada com sucesso.");
        }
        #endregion
    }
}
