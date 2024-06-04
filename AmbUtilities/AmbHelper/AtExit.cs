using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AmbHelper;

public static class AtExit
{
    private static List<Action> _actions = new List<Action>();

    static AtExit()
    {
        AppDomain.CurrentDomain.ProcessExit += (sender, e) =>
        {
            foreach (var action in _actions)
            {
                action();
            }
        };
    }

    public static void Add(Action action)
    {
        _actions.Add(action);
    }
}
