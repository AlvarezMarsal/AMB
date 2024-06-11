namespace AmbHelper;

/// <summary>
/// Each columnin the spreadsheet can be set up as a data source for a geographic location.
/// The definition is loaded from the settings file and then massaged to make
/// processing quicker.
/// </summary>
public class BaseColumnDefinition
{
    public readonly Settings.Column SettingsDefinition;
    public string Tag => SettingsDefinition.Tag;
    public string ColumnName => SettingsDefinition.Name;
    public bool Optional => SettingsDefinition.Optional;
    public bool MustExist => SettingsDefinition.MustExist;
    public readonly int ColumnNumber;
    public string CurrentValue { get; set; } = "";
    public bool IsSystemOwned => SettingsDefinition.IsSystemOwned;
    public List<string> Exclusions => SettingsDefinition.ExcludedValues;
    public bool BlankIsDitto => SettingsDefinition.BlankIsDitto;

    public BaseColumnDefinition(Settings.Column columnDefinition, bool spreadsheetIsOneBased)
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
    public static ColumnDefinitionCollection PreprocessColumnDefinitions(Settings.Import importInfo, bool spreadsheetIsOneBased)
    {
        var list = new ColumnDefinitionCollection();

        // Parse our column definitions
        foreach (var icd in importInfo.Columns)
        {
            var cd = new ColumnDefinition(icd, spreadsheetIsOneBased);
            if (!list.TryAdd(cd))
                throw new InvalidOperationException($"Duplicate column {cd.ColumnName}");
        }

        // Rearrange the 'AliasOf' and 'ParentOf' data
        foreach (var cd in list)
        {
            foreach (var childColumnName in cd.SettingsDefinition.ParentOf)
            {
                if (!list.TryGetValue(childColumnName, out var child))
                    throw new InvalidOperationException($"Unknown parent {childColumnName}");
                if (child.Parent != null)
                    throw new InvalidOperationException($"Duplicate parent {childColumnName}");
                child.Parent = cd;
            }

            if (cd.SettingsDefinition.AliasOf != null)
            {
                if (!list.TryGetValue(cd.SettingsDefinition.AliasOf, out var aliased))
                    throw new InvalidOperationException($"Unknown alias {cd.SettingsDefinition.AliasOf}");
                cd.AliasOf = aliased;
            }
        }

        return list;
    }
}

/// <summary>
/// Each columnin the spreadsheet can be set up as a data source for a geographic location.
/// The definition is loaded from the settings file and then massaged to make
/// processing quicker.
/// </summary>
public class ColumnDefinition : BaseColumnDefinition
{
    public ColumnDefinition? Parent = null;
    public ColumnDefinition? AliasOf = null;
    public object? AssignedValue { get; set; }

    public ColumnDefinition(Settings.Column columnDefinition, bool spreadsheetIsOneBased)
        : base(columnDefinition, spreadsheetIsOneBased)
    {
    }
}


/// <summary>
/// Each columnin the spreadsheet can be set up as a data source for a geographic location.
/// The definition is loaded from the settings file and then massaged to make
/// processing quicker.
/// </summary>
public class ColumnDefinition<T> : BaseColumnDefinition
{
    public ColumnDefinition<T>? Parent = null;
    public ColumnDefinition<T>? AliasOf = null;
    public T? AssignedValue { get; set; }

    public ColumnDefinition(Settings.Column columnDefinition, bool spreadsheetIsOneBased)
        : base(columnDefinition, spreadsheetIsOneBased)
    {
    }
}
