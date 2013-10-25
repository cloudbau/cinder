# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright (c) 2013 Cloudbau, GmbH.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
"""Xtreemfs (http://www.xtreemfs.org/) driver for cinder."""

import errno
import os

from oslo.config import cfg

from cinder.brick.remotefs import remotefs
from cinder import exception
from cinder.openstack.common import log as logging
from cinder import units
from cinder import utils
from cinder.volume.drivers import nfs


LOG = logging.getLogger(__name__)

VOLUME_OPTS = [
    cfg.StrOpt('xtreemfs_shares_config',
               default='/etc/cinder/xtreemfs_shares',
               help='File with the list of available xtreemfs shares, '
               'i.e. <xtreemfs-dir-service-url>/<share-name>'),
    cfg.StrOpt('xtreemfs_mount_point_base',
               default='$state_path/mnt',
               help='Base dir where xtreemfs expected to be mounted'),
    cfg.BoolOpt('xtreemfs_sparsed_volumes',
                default=True,
                help=('Create volumes as sparsed files which take no space.'
                      'If set to False volume is created as regular file.'
                      'In such case volume creation takes a lot of time.'))
]

VERSION = '1.0'

CONF = cfg.CONF
CONF.register_opts(VOLUME_OPTS)


class XtreemfsDriver(nfs.RemoteFsDriver):
    """Xtreemfs based cinder driver."""

    def __init__(self, *args, **kwargs):
        super(XtreemfsDriver, self).__init__(*args, **kwargs)
        self.configuration.append_config_values(VOLUME_OPTS)
        self._remotefsclient = remotefs.RemoteFsClient(
            'xtreemfs', utils.get_root_helper(),
            execute=self._execute,
            xtreemfs_mount_point_base=
                self.configuration.xtreemfs_mount_point_base)
        self._mounted_shares = []
        self._states = {}

    def do_setup(self, context):
        """Check configuration option correctness and mount all available
        shares locally.

        """
        # Check correct configuration.
        config = self.configuration.xtreemfs_shares_config
        if not config:
            msg = (_('Missing configuration option "%s"') %
                   'xtreemfs_shares_config')
            LOG.error(msg)
            raise exception.XtreemfsException(msg)
        if not os.path.exists(config):
            msg = (_("Xtreemfs shares config file at %s doesn't exist") %
                   config)
            LOG.error(msg)
            raise exception.XtreemfsException(msg)
        # Check if XtreemFS is installed.
        try:
            self._execute("mount.xtreemfs", check_exit_code=False,
                          run_as_root=True)
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                msg = _('%s is not installed') % "mount.xtreemfs"
                LOG.error(msg)
                raise exception.XtreemfsException(msg)
            else:
                raise
        # Mount all available shares locally.
        for share in self._load_shares_from_config():
            try:
                self._mount_xtreemfs(share)
                self._mounted_shares.append(share)
            except Exception as exc:
                LOG.error(
                    _('Exception during mounting share %(share)s: %(exc)s'),
                    {'share': share, 'exc': exc})
        # If no shared was mounted successfully raise an error.
        if not self._mounted_shares:
            raise exception.XtreemfsNoSharesMounted()

        LOG.debug(_('Available shares: %r'), self._mounted_shares)

    def _load_shares_from_config(self):
        """Read and return available shares from configuration file."""
        with open(self.configuration.xtreemfs_shares_config) as conf:
            return [share.strip() for share in conf
                      if share and not share.startswith('#')]

    def _get_mount_point_for_share(self, share):
        """Return local mount point for given share.

        :param xtreemfs_share: example 10.123.0.10:/var/xtreemfs
        """
        return self._remotefsclient.get_mount_point(share)

    def _mount_xtreemfs(self, share):
        """Mount XtreemFS share to mount_path."""
        self._remotefsclient.mount(share)

    def _find_share(self, volume_size):
        """Find XtreemFS share that will host given volume size.

        Current implementation looks for share with the greatest available
        capacity.

        :param volume_size: int size in GB
        """
        biggest_size = 0
        biggest_share = None

        for xtreemfs_share in self._mounted_shares:
            capacity, free, used = self._get_capacity_info(xtreemfs_share)
            if free > biggest_size:
                biggest_share = xtreemfs_share
                biggest_size = free

        # Raise an error if volume size can not be hosted in any share.
        if volume_size * units.GiB > biggest_size:
            raise exception.XtreemfsNoSuitableShareFound(
                volume_size=volume_size)
        return biggest_share

    def create_volume(self, volume):
        """Creates a volume in a chosen XtreemFS share.

        Creates file on Xtreemfs share for using it as block device.
        """
        volume['provider_location'] = self._find_share(volume['size'])
        LOG.info(_('Share "%(provider_location)s" was chosen to host'
                 ' volume "%(name)s"'), volume)
        self._do_create_volume(volume)

        return {'provider_location': volume['provider_location']}

    def _do_create_volume(self, volume):
        """Create a volume on given xtreemfs share."""
        volume_path = self.local_path(volume)
        volume_size = volume['size']

        if self.configuration.xtreemfs_sparsed_volumes:
            self._create_sparsed_file(volume_path, volume_size)
        else:
            self._create_regular_file(volume_path, volume_size)

    def delete_volume(self, volume):
        """Deletes a volume."""
        if not volume['provider_location']:
            LOG.error(_('Volume %s does not have provider_location specified, '
                      'abort deleting'), volume['name'])
            return
        mounted_path = self.local_path(volume)
        self._execute('rm', '-f', mounted_path)

    def ensure_export(self, ctx, volume):
        """Synchronously recreates an export for a logical volume."""
        self._mount_xtreemfs(volume['provider_location'])

    def create_export(self, ctx, volume):
        """Override to do nothing"""
        pass

    def remove_export(self, ctx, volume):
        """Override to do nothing."""
        pass

    def initialize_connection(self, volume, connector):
        """Return connection info."""
        return {
            'driver_volume_type': 'xtreemfs',
            'data': {
                'export': volume['provider_location'],
                'name': volume['name']
            }
        }

    def terminate_connection(self, volume, connector, **kwargs):
        """Override to do nothing."""
        pass

    def _get_capacity_info(self, xtreemfs_share):
        """Return total and free and used space of the XtreemFS share
        as a tuple of floats.

        :param xtreemfs_share: example 10.123.0.10:/var/xtreemfs
        """
        mount_point = self._get_mount_point_for_share(xtreemfs_share)

        df, _ = self._execute('stat', '-f', '-c', '%S %b %a', mount_point,
                              run_as_root=True)
        block_size, blocks_total, blocks_avail = map(float, df.split())
        total_available = block_size * blocks_avail
        total_size = block_size * blocks_total

        du, _ = self._execute('du', '-sb', '--apparent-size', '--exclude',
                              '*snapshot*', mount_point, run_as_root=True)
        total_allocated = float(du.split()[0])
        return total_size, total_available, total_allocated

    def get_volume_stats(self, refresh=False):
        """Get volume status. If 'refresh' is True, run update stats first."""
        if refresh:
            self._update_volume_status()
        return self._stats

    def _update_volume_status(self):
        """Retrieve status info from volume group."""
        LOG.debug(_('Updating volume status'))
        # Calculate all shares combined volume capacity.
        global_capacity = 0
        global_free = 0
        for share in self._mounted_shares:
            capacity, free, used = self._get_capacity_info(share)
            global_capacity += capacity
            global_free += free

        backend_name = self.configuration.safe_get('volume_backend_name')
        self._stats = {
            "volume_backend_name": backend_name or self.__class__.__name__,
            "vendor_name": "Open Source",
            "driver_version": "1.0",
            "storage_protocol": "xtreemfs",
            "total_capacity_gb": global_capacity / units.GiB,
            "free_capacity_gb": global_free / units.GiB,
            "reserved_percentage": 0,
            "QoS_support": False
        }
        LOG.debug(_("New stats: %r"), self._stats)
