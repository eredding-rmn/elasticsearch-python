#!/usr/bin/env python
# -*- coding: utf-8 -*-
#  elastic_snapshot.py - making elasticsearch snapshots really easy.
#  elastic_snapshot.py - making elasticsearch snapshots really easy.
#  elastic_snapshot.py - making elasticsearch snapshots really easy.

import sys
import yaml
import datetime
import logging
import logging.handlers
from argparse import ArgumentParser
from lockfile import LockError
from lockfile.pidlockfile import PIDLockFile
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError

try:
    from logging import NullHandler
except ImportError:
    from logging import Handler

    class NullHandler(Handler):
        def emit(self, record):
            pass

LOGFORMAT = "%(asctime)s %(levelname)s: (%(process)d) %(name)s: %(message)s"
SNAPSHOT_DATESTRING = datetime.datetime.now().strftime("%Y%m%d%H%M")
log = logging.getLogger(__name__)


def define_args():
    """ Define command line arguments.
    :return: Argparser object

    in the future, create
     [list|create|remove] [snapshot|<snapshot name>]
    """
    description = 'Trigger a snapshot in a pre-defined elasticsearch repository'
    std_args = ArgumentParser(description=description)
    std_args.add_argument("--host", default='localhost', help='elasticsearch host fqdn or IP')
    std_args.add_argument("--port", default='9200', help='elasticsearch listener port for HOST')
    # assuming path to elasticsearch is / for now..
    #std_args.add_argument("--path", default=None, help="path to elasticsearch (if it's not /)")
    std_args.add_argument("--config", default='/etc/elasticsearch/elasticsearch.yml',
                          help="yaml file for resource specifics")
    std_args.add_argument("--name", default='snapshot-{0}'.format(SNAPSHOT_DATESTRING),
                          help="snapshot name; defaults to '<cluster name>-<region>-{0}".format(SNAPSHOT_DATESTRING))
    std_args.add_argument("--repository", default=None,
                          help="override the discovery of the first (and ideally only) respository")
    std_args.add_argument("--lockfile", default="/var/lock/make_snapshot.py.lock",
                          help="override the default lockfile")
    std_args.add_argument("--dry-run", action='store_true', default=False,
                          help="act like you're going to do it...")
    std_args.add_argument('--debug', action='store_true', default=False,
                          help="Include debugging messages in output")
    return std_args


def resolve_args(args):

    hosts = [{'host': args.host, 'port': args.port}]
    # read elasticsearch config....
    try:
        if args.config:
            config_stream = file(args.config, 'r')
            args.es_config_data = yaml.safe_load(config_stream)
        else:
            log.error('No Config File specified!')
    except yaml.YAMLError, exc:
        if hasattr(exc, 'problem_mark'):
            mark = exc.problem_mark
            print "Error position: (%s:%s)" % (mark.line + 1, mark.column + 1)
        else:
            log.error('Error loading configuration! Bailing!')
        print 'Failure loading configuration'
        sys.exit(1)
    # set up elasticsearch client
    args.esclient = Elasticsearch(
        hosts,
        # sniff before doing anything
        sniff_on_start=True,
        # refresh nodes after a node fails to respond
        sniff_on_connection_fail=True,
        # and also every 60 seconds
        sniffer_timeout=60
    )
    try:
        args.cluster_state = args.esclient.cluster.state(metric='nodes', local=True)
    except ConnectionError as e:
        log.error('Error connecting to {0}:{1}: {2}'.format(args.host, args.port, e.message))

    args.cluster_master = args.esclient.cluster.state(metric='master_node')

    args.local_node_name = args.esclient.nodes.info(node_id=['_local']).get('nodes').keys().pop()
    if args.cluster_master.get('master_node') == args.local_node_name:
        args.is_local = True
    else:
        args.is_local = False

    args.indexes = args.esclient.indices.get_settings('_all', params={'expand_wildcards': 'closed'}).keys()
    if not args.repository:
        repodict = args.esclient.snapshot.get_repository()
        repo = repodict.keys().pop()
        args.repository = repo
    # get current snapshots...
    args.current_snapshots = args.esclient.snapshot.get(repository=args.repository, snapshot='_all', master_timeout=30)
    if args.current_snapshots:
        args.snapshots = [snap.get('snapshot') for snap in args.current_snapshots.get('snapshots')]
    else:
        args.snapshots = None
    args.lock = PIDLockFile(args.lockfile)
    return args


def is_healthy(args):
    """ return a tuple of cluster_name and bool to explain state
    """
    health_report = args.esclient.cluster.health()
    log.debug('health report: {0}'.format(health_report))
    if health_report.get('status') == 'green':
        return True
    else:
        return False


def make_snapshot(args):
    """ create a snapshot..
    """

    snapshot_body = {"ignore_unavailable": "true", "include_global_state": False}
    log.debug('STARTED CREATE SNAPSHOT')
    snap_params = {
        'repository': args.repository,
        'snapshot': args.name,
        'body': snapshot_body,
        'master_timeout': 1800,
        'wait_for_completion': True
    }
    if args.dry_run:
        log.info("NOOP: would have executed snapshot with data: {0}".format(snap_params))
        return True
    rval = args.esclient.snapshot.create(**snap_params)
    log.debug('FINISHED CREATE SNAPSHOT; rval: {0}'.format(rval))
    return rval


if __name__ == '__main__':
    argparser = define_args()
    args = argparser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO, format=LOGFORMAT, datefmt="%Y-%m-%dT%H:%M:%S")
    if args.debug:
        logging.getLogger("elasticsearch").propagate = True
        logging.getLogger("urllib3").propagate = True
    else:
        logging.getLogger("elasticsearch").propagate = False
        logging.getLogger("urllib3").propagate = False

    resolve_args(args)

    if args.debug:
        log.debug("Command: " + " ".join(sys.argv))
        log.debug(" === Current Configuration ===")
        for attrib, val in args.__dict__.iteritems():
            log.debug("{0}: {1}   ".format(attrib, val))

    if not args.is_local:
        log.warn('WARN:  Exiting; node {0} does not match cluster master node, {1}'.format(
            args.local_node_name, args.cluster_master.get('master_node'))
        )
        sys.exit(0)
    # check if node is healthy,
    if not is_healthy(args):
        log.error("ERROR: Health state is not 'green'; cannot create snapshot at this time!")
        sys.exit(1)

    try:
        with args.lock:
            result = make_snapshot(args)
        print result
    except LockError as e:
        log.error('ERROR: unable to aquire lock file {0}; is there another process running?'.format(args.lockfile))
        sys.exit(1)
