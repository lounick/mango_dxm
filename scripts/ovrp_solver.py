from __future__ import (absolute_import, division, print_function, unicode_literals)
import numpy as np
from pulp import *
import time
import matplotlib as mpl
import matplotlib.pyplot as plt

from mpl_toolkits.mplot3d import Axes3D

np.set_printoptions(precision=3, suppress=True)

def _set_plot_style():
    """Set the global matplotlib using the project's default style.

    Notes
    -----
    This configuration affects all the tests and the examples included in this package.
    """
    if 'bmh' in mpl.style.available:
        mpl.style.use('bmh')

    mpl.rcParams['figure.figsize'] = _get_figsize(scale=2.0)
    #mpl.rcParams[''] = 'tight'

def _get_figsize(scale=1.0):
    """Calculate figure size using aestetic ratio.

    Parameters
    ----------
    scale : float
        Scaling parameter from basic aspect (6.85:4.23).

    Returns
    -------
    figsize : tuple
        A 2-element tuple (w, h) defining the width and height of a figure.

    Examples
    --------
    This function is used to configure matplotlib.

    >>> import numpy as np
    >>> np.allclose(_get_figsize(1.0), (6.85, 4.23), atol=0.1)
    True
    """
    fig_width_pt = 495.0                              # Get this from LaTeX using \the\textwidth
    inches_per_pt = 1.0 / 72.27                       # Convert pt to inch

    golden_mean = (np.sqrt(5.0) - 1.0) / 2.0

    fig_width = fig_width_pt * inches_per_pt * scale  # width in inches
    fig_height = fig_width * golden_mean              # height in inches

    return (fig_width, fig_height)

def calculate_distances(nodes):
    n = np.atleast_2d(nodes).shape[0]
    distances = np.zeros((n, n))

    for k in xrange(n):
        for p in xrange(n):
            distances[k, p] = np.linalg.norm(nodes[k, :] - nodes[p, :])

    return distances

def plot_problem(nodes, solution, objective):
    """Plot the results of a problem in 2D coordinates (X-Y).

    Parameters
    ----------
    nodes
    solution
    objective

    Returns
    -------
    fig: object
        figure object

    ax: object
        axes object
    """
    # init plot
    _set_plot_style()
    fig, ax = plt.subplots()

    # plot vertexes
    ax.plot(nodes[:, 1], nodes[:, 0], 'o', ms=8, label='nodes')

    # # add labels
    # for n in xrange(len(idx)):
    #     x, y = nodes[n, 1], nodes[n, 0]
    #     xt, yt = x - 0.10 * np.abs(x), y - 0.10 * np.abs(y)
    #
    #     ax.annotate('#%d' % n, xy=(x, y), xycoords='data', xytext=(xt,yt))

    # normalize solution(s)
    indexes = []

    if len(solution) > 0:
        if type(solution[0]) == list:
            indexes.extend(solution)
        else:
            indexes.append(solution)

    # plot solution(s)
    for n, idx in enumerate(indexes):
        # route plots
        route = nodes[idx, :]
        ax.plot(route[:, 1], route[:, 0], '--', alpha=0.8, label='route #{}'.format(n))

        # add route order
        for k, n in enumerate(idx):
            x, y = nodes[n, 1], nodes[n, 0]
            xt, yt = x + 0.05 * np.abs(x), y + 0.05 * np.abs(y)

            ax.annotate(str(k), xy=(x, y), xycoords='data', xytext=(xt, yt))

    # adjust plot features
    xlim = ax.get_xlim()
    ylim = ax.get_ylim()
    xnew =  (xlim[0] - np.abs(xlim[0] * 0.05), xlim[1] + np.abs(xlim[1] * 0.05))
    ynew =  (ylim[0] - np.abs(ylim[0] * 0.05), ylim[1] + np.abs(ylim[1] * 0.05))

    ax.set_xlim(xnew)
    ax.set_ylim(ynew)

    ax.legend(loc='best')
    ax.xaxis.set_minor_locator(mpl.ticker.AutoMinorLocator(2))
    ax.yaxis.set_minor_locator(mpl.ticker.AutoMinorLocator(2))
    ax.grid(which='minor')

    ax.set_xlabel('X (m)')
    ax.set_ylabel('Y (m)')
    ax.set_title('Problem Solution')

    return fig, ax

def ovrp_solver(cost, start=None, finish=None, **kwargs):
    # Number of points
    n = cost.shape[0]

    # Check for default values
    if start is None:
        start = str(0)
    else:
        start = str(start)
    if finish is None:
        finish = str(n - 1)
    else:
        finish = str(finish)

    tmp_cities = range(n)
    cities = [str(tmp_cities[i]) for i in range(len(tmp_cities))]

    costs = pulp.makeDict([cities, cities], cost, 0)

    routes = []
    for i in cities:
        for j in cities:
            if i != j:
                routes.append((i, j))

    prob = pulp.LpProblem("OVRP", pulp.LpMinimize)

    e_vars = pulp.LpVariable.dicts("e", (cities, cities), lowBound=0, upBound=1, cat=pulp.LpBinary)
    u_vars = pulp.LpVariable.dicts("u", cities[:], lowBound=0, upBound=len(cities)+1, cat=pulp.LpInteger)

    prob += sum([e_vars[w][b] * costs[w][b] for (w, b) in routes]), "Sum_of_Tour_Costs"

    for i in cities:
        tmp = []
        if i != finish:
            for j in cities:
                if i != j:
                    tmp.append(e_vars[i][j])
            prob += sum(tmp) == 1, "route_out_%s" % i

    tmp = []
    for i in cities:
        if i != finish:
            tmp.append(e_vars[finish][i])
    prob += sum(tmp) == 0, "route_out_%s" % cities[int(finish)]

    for i in cities:
        tmp = []
        if i != start:
            for j in cities:
                if i != j:
                    tmp.append(e_vars[j][i])
            prob += sum(tmp) == 1, "route_in_%s" % i

    tmp = []
    for i in cities:
        if i != start:
            tmp.append(e_vars[i][start])
    prob += sum(tmp) == 0, "route_in_%s" % cities[int(start)]

    for i in cities:
        for j in cities:
            if i != j:
                prob += u_vars[i] - u_vars[j] + n * e_vars[i][j] <= n - 1

    print("Solving problem")
    start = time.time()
    prob.solve()
    end = time.time()
    print('Solving problem', 'took', end - start, 'time')

    print("Status:", pulp.LpStatus[prob.status])

    # for v in prob.variables():
    #     print(v.name, "=", v.varValue)

    # print(prob.variables())
    # for k,v in prob.variablesDict().iteritems():
    #     print(k, v)
    #
    var_dict = prob.variablesDict()
    for i in cities:
        for j in cities:
            # if i != j and var_dict[e_vars[i][j].name].varValue > 0:
            if e_vars[i][j].value() > 0:
                print(e_vars[i][j])

    # extract calculated route
    route = np.zeros(n, dtype=np.int)
    for i in cities:
        route[u_vars[i].value()] = int(i)
    print(route)

    print("Total Cost of Transportation = ", prob.objective.value())
    return route, prob.objective.value()

def main():
    import matplotlib.pyplot as plt


    nodes = []
    nodes.append([0, 0])

    for i in range(1, 4):
        for j in range(-1, 2):
            ni = i
            nj = j
            # ni = random.uniform(-0.5,0.5) + i
            # nj = random.uniform(-0.5,0.5) + j
            nodes.append([ni, nj])

    nodes.append([4, 0])
    nodes = np.array(nodes)

    cost = calculate_distances(nodes)

    solution, cost_total = ovrp_solver(cost)
    print(cost_total)
    fig, ax = plot_problem(nodes, solution, cost_total)
    plt.show()


if __name__ == '__main__':
    main()