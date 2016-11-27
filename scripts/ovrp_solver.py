from __future__ import (absolute_import, division, print_function, unicode_literals)
import numpy as np
import pulp
from pulp import *
import time

def calculate_distances(nodes):
    n = np.atleast_2d(nodes).shape[0]
    distances = np.zeros((n, n))

    for k in xrange(n):
        for p in xrange(n):
            distances[k, p] = np.linalg.norm(nodes[k, :] - nodes[p, :])

    return distances

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
    u_vars = pulp.LpVariable.dicts("u", cities[:], lowBound=0, upBound=len(cities)-1, cat=pulp.LpInteger)

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
    prob.solve(pulp.GLPK(msg=0))
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
    nodes = []
    nodes.append([0, 0])

    for i in range(1, 4):
        for j in range(-1, 2):
            nodes.append([i, j])

    nodes.append([4, 0])
    nodes = np.array(nodes)

    cost = calculate_distances(nodes)

    solution, cost_total = ovrp_solver(cost)
    print(cost_total)

if __name__ == '__main__':
    main()