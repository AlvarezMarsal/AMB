namespace ImportLocations;

internal class GeographicLocation
{
    public readonly long Oid;
    public readonly long? Pid;
    public readonly string Name;
    public readonly int Index;
    public readonly string Description;
    public readonly long PracticeAreaId;
    public readonly bool IsSystemOwned;
    public readonly SortedList<string, GeographicLocation> Children = [];
    public override string ToString() => $"{Name} ({Oid})";
    public bool ChildrenLoaded { get; set; } = false;

    public GeographicLocation(long oid, long? pid, string name, int index, string description, long practiceAreaId, bool isSystemOwned)
    {
        Oid = oid;
        Pid = pid;
        Oid = oid;
        Name = name;
        Index = index;
        Description = description;
        PracticeAreaId = practiceAreaId;
        IsSystemOwned = isSystemOwned;
    }

    public void Walk(Action<GeographicLocation> action)
    {
        action(this);
        foreach (var child in Children)
        {
            child.Value.Walk(action);
        }
    }
}

