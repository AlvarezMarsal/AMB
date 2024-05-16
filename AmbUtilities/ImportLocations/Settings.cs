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
        public string Range { get; set; } = "";
        public string Name { get; set; } = "";                      // The column that the entity's name is in
        public List<string> Aliases { get; set; } = [];             // The columns that the entity's aliases are in
        public List<HierarchyInfo> Hierarchy { get; set; } = [];    // The entity's parents, grandparents, etc. in ascending order of size (world is last)
    }

    public class HierarchyInfo
    {
        public string? Column { get; set; } = "";            // The column that the parent's name is in ...
        public long? Oid { get; set; }                       // ... or its OID
        public List<string> Aliases { get; set; } = [];     // We can assign aliases to parents, too
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
}
