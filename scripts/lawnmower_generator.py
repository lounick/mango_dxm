#!/usr/bin/python

import numpy as np
import trajectory_generator as tg

from numpy import cos, sin


def pattern_from_rect(width, height, delta=0, start=0):
    """
        area = [    
            A[n ,e, d],
            B[n ,e, d],
            C[n ,e, d],
            D[n ,e, d]
        ]

        
    area: represent the bounding box of the lawnmower pattern
        A ------------------- B
        |                     |
        |                     |
        |                     |
        D ------------------- C

    
    returns (points, n_cols): 
    where points are the lawnmower pattern points within the bounding box

        1       3 ----- 5
        |       |       |
        |       |       |
        2 ----- 4       6 ----- *

    """
    print("[width, height, delta, start] = [{0}, {1}, {2}, {3}]".format(width, height, delta, start))

    # trajectory matrix
    n_cols = np.ceil(width / delta) + 1      # number of columns (+ final)
    n_npc = 2                                # number of points per column
    n_points = n_npc * n_cols                # number of matrix rows

    # init empty trajectory
    print(n_points)
    trajectory = np.zeros((n_points, 6))
    depth = 0
    east = 0
    idx = 0

    # trajectory loop
    for n in np.arange(0, n_cols):  
        idx = n_npc * n
        if start == 0 or start == 1:
            if n % 2 == 0:
                # first leg
                trajectory[idx, :] = np.array([0, east, depth, 0, 0, np.pi])
                trajectory[idx+1, :] = np.array([-height, east, depth, 0, 0, np.pi])
            else:
                # second leg
                trajectory[idx, :] = np.array([-height, east, depth, 0, 0, 0.0])
                trajectory[idx+1, :] = np.array([0, east, depth, 0, 0, 0.0])
        else:
            if n % 2 == 0:
                # first leg
                trajectory[idx, :] = np.array([0, east, depth, 0, 0, 0.0])
                trajectory[idx+1, :] = np.array([-height, east, depth, 0, 0, 0.0])
            else:
                # second leg
                trajectory[idx, :] = np.array([-height, east, depth, 0, 0, np.pi])
                trajectory[idx+1, :] = np.array([0, east, depth, 0, 0, np.pi])

        # update east for next pair of legs
        east += delta

    # handle last column
    if trajectory[idx, 1] > width:
        trajectory[idx, 1] = width
        trajectory[idx+1, 1] = width

    # rotate and translate according to the starting point
    # if start == 0:
    #     trajectory[:, -1] += np.pi
    if start == 1:
        # starting point is B
        trajectory[:,1] = (-trajectory[:,1]) + width
        # trajectory[:, -1] += np.pi

    elif start == 2:
        # starting point is C
        trajectory[:,0] = (-trajectory[:,0]) - height
        trajectory[:,1] = (-trajectory[:,1]) + width

    elif start == 3:
        # starting point is D
        trajectory[:,0] = (-trajectory[:,0]) - height

    else:
        pass

    return (trajectory, n_cols)


def pattern_from_ned(area, start=0, spacing=2, overlap=0):
    # # check A,B,C,D are in the expected order and A,B,C,D[depth] are equals
    # conditions = [
    #     np.all(area[0,0] > area[2:4,0]),    # A[north] > C,D[north]
    #     np.all(area[0,1] < area[1:3,1]),    # A[east] < B,C[east]
    #     area[0,0] == area[1,0],             # A[north] == B[north]
    #     area[0,1] == area[3,1],             # A[east] == D[east]
    #
    #     np.all(area[2,0] < area[0:2,0]),    # C[north] < A,B[north]
    #     np.all(area[2,1] > area[[0,3],1]),  # C[east] > A,D[east]
    #     area[2,0] == area[3,0],             # C[north] == D[north]
    #     area[2,1] == area[1,1],             # C[east] == B[east]
    #
    #     np.all(area[0,2] == area[:,2])      # depth are equals
    # ]
    #
    # if not np.all(conditions):
    #     raise ValueError('area is not rectangular shaped!')
    print("[start, spacing, overlap] = [{0}, {1}, {2}]".format(start, spacing, overlap))

    # TODO: insert another condition like ab == cd and bc == da
    a = np.linalg.norm(area[0,0:2] - area[1,0:2])   # AB
    b = np.linalg.norm(area[1,0:2] - area[2,0:2])   # BC
    c = np.linalg.norm(area[2,0:2] - area[3,0:2])   # CD
    d = np.linalg.norm(area[3,0:2] - area[0,0:2])   # DA

    diag_1 = (a**2 + b**2)
    diag_2 = (c**2 + d**2)

    if not np.abs(diag_2 - diag_1) < 1e-6:
        raise ValueError('area is not rectangular shaped!')

    # calculate bounding box dimensions
    dW = area[1,:2] - area[0,:2]    # B - A (xy)
    dH = area[3,:2] - area[0,:2]    # D - A (xy)

    width = np.sqrt(np.dot(dW, dW))
    height = np.sqrt(np.dot(dH, dH))
    depth = np.abs(area[0,2])

    # calculate delta from range and overlap
    delta = float(spacing - (spacing * overlap))

    # generate using simple geometry
    (fixes, n_cols) = pattern_from_rect(width, height, delta, start)

    # adjust depth
    fixes[:,2] = depth

    # translations (using A point to get correct position in space)
    fixes[:,0] += area[0,0]
    fixes[:,1] += area[0,1]

    # correct initial yaw
    dE = area[1,1] - area[0,1]              # delta_east between B and A
    alpha = np.arccos(dE / width)           # angle between B and A respect to NED reference
    fixes[:,5] += alpha                     # add the rotation to fixes

    # rotate waypoints
    ROT = np.eye(6)

    # set the rotation using current attitude
    ROT[0:2, 0:2] = [
        [cos(alpha),    sin(alpha)],
        [-sin(alpha),   cos(alpha)]
    ]

    for n in range(fixes.shape[0]):
        fixes[n,:] = np.dot(ROT, fixes[n,:])

    return (fixes, n_cols)


def main():
    area = np.array([
        [0,0,2],            # A
        [0,40,2],           # B
        [-20,40,2],         # C
        [-20,0,2]           # D
    ])

    # rotation case
    area = np.array([
        [0,0,2],            # A
        [10,10,2],          # B
        [0, np.sqrt(2) * 10,2],            # C
        [-10,10,2]          # D
    ])

    # sonar parameters
    sonar_field = 2        # meters
    sonar_overlap = 0       # 0 to 1

    # get global points
    (fixes_a, n_cols) = pattern_from_ned(area, start=0, spacing=sonar_field, overlap=sonar_overlap)
    tg.print_as_json(fixes_a, spec='pandora')

    (fixes_b, n_cols) = pattern_from_ned(area, start=1, spacing=sonar_field, overlap=sonar_overlap)
    tg.print_as_json(fixes_b, spec='pandora')

    (fixes_c, n_cols) = pattern_from_ned(area, start=2, spacing=sonar_field, overlap=sonar_overlap)
    tg.print_as_json(fixes_c, spec='pandora')

    (fixes_d, n_cols) = pattern_from_ned(area, start=3, spacing=sonar_field, overlap=sonar_overlap)
    tg.print_as_json(fixes_d, spec='pandora')

    # test cases
    from matplotlib import pylab

    fig = pylab.figure()
    pylab.plot(fixes_a[:,1], fixes_a[:,0], 'or-')
    pylab.plot(fixes_b[:,1], fixes_b[:,0], '*g--')
    # pylab.plot(fixes_c[:,1], fixes_c[:,0], 'ob-')
    # pylab.plot(fixes_d[:,1], fixes_d[:,0], 'oy-')
    pylab.grid()
    pylab.show()

if __name__ == '__main__':
    main()
