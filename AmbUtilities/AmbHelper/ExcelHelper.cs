using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AmbHelper;

public static class ExcelHelper
{
    public static int ColumnAlphaToColumnNumber(string column, bool spreadsheetIsOneBased)
    {
        var result = 0;

        foreach (var ch in column)
        {
            if (!char.IsLetter(ch))
                break;
            if (char.IsLower(ch))
                throw new ArgumentException("Invalid column name");
            result = result * 26 + (ch - 'A');
        }

        return result + (spreadsheetIsOneBased ? 1 : 0);
    }
}
