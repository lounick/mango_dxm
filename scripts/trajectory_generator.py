#!/usr/bin/env python

import sys
import os
import json
import datetime

import numpy as np
from numpy import cos, sin


def smoothed_trajectory(A, B, chunks=2, interp='linear'):
    """smoothed trajectory between A and B"""

    if chunks < 2:
        raise ValueError('need at least two chunks to smooth the trajectory!')

    # intermediate points
    legs = np.zeros((chunks, 6))

    # north, east, depth
    legs[:, 0] = np.linspace(A[0], B[0], num=chunks)
    legs[:, 1] = np.linspace(A[1], B[1], num=chunks)
    legs[:, 2] = np.linspace(A[2], B[2], num=chunks)

    # roll and pitch (actually not enforced in AUV)
    legs[:, 3] = np.linspace(A[3], B[3], num=chunks)
    legs[:, 4] = np.linspace(A[4], B[4], num=chunks)

    # NOTE: extra care is needed with yaw rotations
    #   case 1: use the long read (not optimized if A and B are distant)
    #   case 2: use brute force and rotate first
    if interp.lower() == 'brute':
        legs[:, 5] = B[5] * np.ones(chunks)
    else:
        legs[:, 5] = np.linspace(A[5], B[5], num=chunks)

    return legs


# def calculate_trajectory(A, B, unit=1, chunks=None):
#     '''smoothed trajectory between A and B'''
#
#     if unit is not None and chunks is None:
#         (delta, distance) = calculate_delta(A,B)
#         steps = np.ceil(distance / unit)
#     else:
#         steps = chunks + 1
#
#     # intermediate points
#     legs = np.zeros((steps, 6))
#
#     # north, east, depth
#     legs[:, 0] = np.linspace(A[0], B[0], num=steps)
#     legs[:, 1] = np.linspace(A[1], B[1], num=steps)
#     legs[:, 2] = np.linspace(A[2], B[2], num=steps)
#
#     # roll and pitch (actually not enforced in AUV)
#     legs[:, 3] = np.linspace(A[3], B[3], num=steps)
#     legs[:, 4] = np.linspace(A[4], B[4], num=steps)
#
#     # TODO: extra care for yaw rotations
#     #   case 1: use the long read (not optimized if A and B are distant)
#     #   case 2: use brute force and rotate first
#     #   case 3: be smart and mix the above two
#
#     # take the long road
#     legs[:, 5] = np.linspace(A[5], B[5], num=steps)
#     return legs


def calculate_delta(A, B):
    """calculate distance and delta vector in body frame coordinates at point A"""

    # initial orientation
    r = A[3]    # phi
    p = A[4]    # theta
    y = A[5]    # psi

    # rotation matrix for body frame deltas
    ROT = np.eye(6)

    # set the rotation using current attitude
    ROT[0:2, 0:2] = [
        [cos(p)*cos(y),    cos(r)*sin(y)+sin(r)*sin(p)*cos(y)],
        [-cos(p)*sin(y),   cos(r)*cos(y)-sin(r)*sin(p)*sin(y)]
    ]

    # body frame rotation
    A_body = np.dot(A, np.linalg.inv(ROT))
    B_body = np.dot(B, np.linalg.inv(ROT))

    # delta in global reference
    delta = abs(B - A)

    # distance between A and B
    distance = np.linalg.norm(delta)
    
    return (delta, distance)


def export_as_json(points, spec='', indent=None):
    # generated trajectory
    trajectory = {}

    if spec.lower() == 'pandora':
        trajectory['poses'] = []

        # trajectory points loop
        for p in points:
            trajectory['poses'].append({
                    'position': p[:3].tolist(),
                    'orientation': p[3:].tolist()
                })

    else:
        trajectory['generated'] = datetime.datetime.now().isoformat()
        trajectory['points'] = points.tolist()

    # final trajectory
    return json.dumps(trajectory, indent=indent)


def print_as_json(points, spec='', indent=None, stripped=False):
    trajectory = export_as_json(points, spec, indent)

    if stripped is True:
        trajectory = trajectory.replace('"','')

    if spec.lower() == 'pandora':
        trajectory = trajectory.replace('},', '},\n')
        trajectory = trajectory.replace(']}]}', ']}\n]}')
        trajectory = trajectory.replace('"poses": [{', '"poses": [\n {')

    # send to sysout
    print(trajectory)



def main():
    # incremental steps
    DW = [ x**2 for x in np.linspace(0, 10, num=10) ]
    
    # yawing
    #DW = np.deg2rad(np.linspace(-180,180,num=30))
    DW = np.deg2rad([180, 150, 120, 90, 60, 30, 0,
     -20, -40, -60, -80, -100, -120, -140, -160, -180,
     -140, -100, -60, -20, 
     0, 60, 120, 180, 
     170, 160, 150, 140, 130, 120, 110, 100, 90, 0
    ])

    # requested inspection point
    WS = np.array([(0,0,0,0,0,x) for x in DW])
    
    # single points
    points = np.zeros((1, 6))

    # trajectory smoothing
    LEG_UNIT = 3    # meters

    for i in range(1, len(WS)):
        (delta, distance) = calculate_delta(WS[i-1], WS[i])         # body frame delta and distance
        steps = max(np.ceil(distance / LEG_UNIT), 2)                # minimum two chunks

        legs = smoothed_trajectory(WS[i-1], WS[i], chunks=steps)
        points = np.concatenate((points, legs), axis=0)

    # final trajectory
    print_as_json(points[1:,:], spec='pandora')

    # clean exit
    sys.exit(0)


if __name__ == '__main__':
    main()
