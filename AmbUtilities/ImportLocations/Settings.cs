using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ImportLocations;

/// <summary>
/// A POCO class to hold settings for the ImportLocations application.
/// </summary>
public class Settings
{
    public class ImportFileInfo
    {
        public string FilePath { get; set; } = "";
        public string Sheet { get; set; } = "";
        public int FirstRow { get; set; } = -1;
        public int LastRow { get; set; } = -1;
        public List<Settings.ColumnDefinition> ColumnDefinitions { get; set; } = [];    // The entity's parents, grandparents, etc. in ascending order of size (world is last)
    }

    // Each column contains a string that is the name or alias of some GeographicalLocation.
    // The relationships among the columns are defined by the ColumnDefinitions.
    public class ColumnDefinition
    {
        public string Tag { get; set; } = "";               // A useful tag like 'Country' or 'City'
        public string Column { get; set; } = "";            // Which column in the spreadsheet
        public List<string> ParentOf { get; set; } = [];    // The value in the column can identify a parent of something in another column
        public string? AliasOf { get; set; } = null;        // The value in the column can be an alias of the entity identified by another column
        public bool MustExist { get; set; } = false;
        public bool Optional { get; set; } = false;
        public bool IsSystemOwned { get; set; } = false;
        public List<string> ExcludedValues { get; set; } = [];
    }

    public class Preset
    {
        public string Name { get; set; } = "";
        public long Oid { get; set; }
        public long? Pid { get; set; }
    }

    public string ConnectionString { get; set; } = "";
    public long CreatorId { get; set; } = 1;
    public string? CreationSession { get; set; } = null;
    public List<Preset> Presets { get; set; } = [];
    public List<ImportFileInfo> ImportFiles { get; set; } = [];
    public string NameCollation { get; set; } = "SQL_Latin1_General_CP1_CI_AS";
    public string AliasCollation { get; set; } = "SQL_Latin1_General_CP1_CI_AS";
}
