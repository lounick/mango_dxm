#! /usr/bin/env python

import sys
from subprocess import call, CalledProcessError
import time
import os

def main(argv):
    db_name = "sunset_" + argv[0]

    while True:
        try:
            retcall = call("rethinkdb" + " dump -e " + db_name, shell=True)
            if retcall < 0:
                print >>sys.stderr, "Child was terminated by signal ", -retcall
            else:
                print >>sys.stderr, "Child returned ", retcall
        except OSError as e:
            print >>sys.stderr, "Call failed ", e
        time.sleep(10)

if __name__ == '__main__':
    main(sys.argv[1:])