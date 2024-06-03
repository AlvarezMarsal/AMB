namespace AmbHelper;

/// <summary>
/// Each columnin the spreadsheet can be set up as a data source for a geographic location.
/// The definition is loaded from the settings file and then massaged to make
/// processing quicker.
/// </summary>
public class ColumnDefinition
{
    public readonly Settings.Column SettingsDefinition;
    public string Tag => SettingsDefinition.Tag;
    public string ColumnName => SettingsDefinition.Name;
    public bool Optional => SettingsDefinition.Optional;
    public bool MustExist => SettingsDefinition.MustExist;
    public readonly int ColumnNumber; 
    public ColumnDefinition? Parent = null;
    public ColumnDefinition? AliasOf = null;
    public object? AssignedValue { get; set; }
    public string CurrentValue { get; set; } = "";
    public bool IsSystemOwned => SettingsDefinition.IsSystemOwned;
    public List<string> Exclusions => SettingsDefinition.ExcludedValues;
    public bool BlankIsDitto => SettingsDefinition.BlankIsDitto;

    public ColumnDefinition(Settings.Column columnDefinition, bool spreadsheetIsOneBased)
    {
        SettingsDefinition = columnDefinition;
        ColumnNumber = ExcelHelper.ColumnAlphaToColumnNumber(SettingsDefinition.Name, spreadsheetIsOneBased);
    }

    public override string ToString()
    {
        return $"{ColumnName} {Tag} '{CurrentValue}'";
    }

    /// <summary>
    /// Retusn a list of ColumnDefinitions in the order they should be processed.
    /// </summary>
    /// <param name="importInfo"></param>
    /// <param name="spreadsheetIsOneBased"></param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    public static List<ColumnDefinition> PreprocessColumnDefinitions(Settings.Import importInfo, bool spreadsheetIsOneBased)
    {
        // Parse our column definitions
        var columnDefinitions = new SortedList<string, ColumnDefinition>(); // columnName -> ColumnDefinition
        foreach (var icd in importInfo.Columns)
        {
            var cd = new ColumnDefinition(icd, spreadsheetIsOneBased);
            if (!columnDefinitions.TryAdd(cd.ColumnName, cd))
                throw new InvalidOperationException($"Duplicate column {cd.ColumnName}");
        }

        // Rearrange the 'AliasOf' and 'ParentOf' data
        foreach (var cd in columnDefinitions.Values)
        {
            foreach (var childColumnName in cd.SettingsDefinition.ParentOf)
            {
                if (!columnDefinitions.TryGetValue(childColumnName, out var child))
                    throw new InvalidOperationException($"Unknown parent {childColumnName}");
                if (child.Parent != null)
                    throw new InvalidOperationException($"Duplicate parent {childColumnName}");
                child.Parent = cd;
            }

            if (cd.SettingsDefinition.AliasOf != null)
            {
                if (!columnDefinitions.TryGetValue(cd.SettingsDefinition.AliasOf, out var aliased))
                    throw new InvalidOperationException($"Unknown alias {cd.SettingsDefinition.AliasOf}");
                cd.AliasOf = aliased;
            }
        }

        return columnDefinitions.Values.ToList();
    }
}


/// <summary>
/// Each columnin the spreadsheet can be set up as a data source for a geographic location.
/// The definition is loaded from the settings file and then massaged to make
/// processing quicker.
/// </summary>
public class ColumnDefinition<T> : ColumnDefinition
{
    public new ColumnDefinition<T>? Parent = null;
    public new ColumnDefinition<T>? AliasOf = null;
    public new T? AssignedValue { get; set; }

    public ColumnDefinition(Settings.Column columnDefinition, bool spreadsheetIsOneBased)
        : base(columnDefinition, spreadsheetIsOneBased)
    {
    }
}
