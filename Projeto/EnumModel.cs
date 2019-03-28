using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProcessETL
{
    public static class EnumModel
    {
        public enum TipoTitulo
        {
            CD = 'C',
            DVD = 'D'
        }

        public enum ClassificacaoTitulo
        {
            Livre = 'L',
            Normal = 'N',
            Promocional = 'P'
        }
    }
}
