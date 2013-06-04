# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright (c) 2013 Cloudbau, Inc.
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

from cinder import exception
from cinder import flags
from cinder.openstack.common import log as logging
from cinder.volume.drivers import nfs


LOG = logging.getLogger(__name__)

VOLUME_OPTS = [
    cfg.StrOpt('xtreemfs_shares_config',
               default="/etc/cinder/shares.txt",
               help='File with the list of available xtreemfs shares'),
    cfg.StrOpt('xtreemfs_mount_point_base',
               default='$state_path/mnt',
               help='Base dir where xtreemfs expected to be mounted'),
    cfg.StrOpt('xtreemfs_disk_util',
               default='df',
               help='Use du or df for free space calculation'),
    cfg.BoolOpt('xtreemfs_sparsed_volumes',
                default=True,
                help=('Create volumes as sparsed files which take no space.'
                      'If set to False volume is created as regular file.'
                      'In such case volume creation takes a lot of time.'))
]
FLAGS = flags.FLAGS
FLAGS.register_opts(VOLUME_OPTS)


class XtreemfsDriver(nfs.RemoteFsDriver):
    """Xtreemfs based cinder driver.

    Creates file on Xtreemfs share for using it as block device on instances.
    """

    def __init__(self, *args, **kwargs):
        super(XtreemfsDriver, self).__init__(*args, **kwargs)
        self.configuration.append_config_values(VOLUME_OPTS)
        self._mounted_shares = []

    def check_for_setup_error(self):
        """Returns an error if prerequisites aren't met"""
        config = self.configuration.xtreemfs_shares_config
        if not config:
            msg = (_("Missing configuration option (%s)") %
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
            self._execute('mount.xtreemfs', check_exit_code=False)
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                msg = _('mount.xtreemfs is not installed')
                LOG.error(msg)
                raise exception.XtreemfsException(msg)
            else:
                raise

    def create_volume(self, volume):
        """Creates a volume in an XtreemFS share."""
        self._ensure_shares_mounted()
        volume['provider_location'] = self._find_share(volume['size'])
        LOG.info(_('casted to %s') % volume['provider_location'])
        self._do_create_volume(volume)

        return {'provider_location': volume['provider_location']}

    def delete_volume(self, volume):
        """Deletes a volume."""
        if not volume['provider_location']:
            LOG.warn(_('Volume %s does not have provider_location specified, '
                     'skipping'), volume['name'])
            return
        self._ensure_share_mounted(volume['provider_location'])
        mounted_path = self.local_path(volume)
        self._execute('rm', '-f', mounted_path)

    def ensure_export(self, ctx, volume):
        """Synchronously recreates an export for a logical volume."""
        self._ensure_share_mounted(volume['provider_location'])

    def create_export(self, ctx, volume):
        """Override to do nothing"""

    def remove_export(self, ctx, volume):
        """Override to do nothing."""

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

    def get_volume_stats(self, refresh=False):
        """Get volume status. If 'refresh' is True, run update stats first."""
        if refresh:
            self._update_volume_status()
        return self._stats

    def _update_volume_status(self):
        """Retrieve status info from volume group."""
        LOG.debug(_("Updating volume status"))
        self._stats = {
            "volume_backend_name": "Xtreemfs",
            "vendor_name": "OpenSrouce",
            "driver_version": "1.0",
            "storage_protocol": "Xtreemfs",
            "total_capacity_gb": "infinite",
            "free_capacity_gb": "infinite",
            "reserved_percentage": 100,
            "QoS_support": False
        }

    def _do_create_volume(self, volume):
        """Create a volume on given xtreemfs share."""
        volume_path = self.local_path(volume)
        volume_size = volume['size']

        if self.configuration.xtreemfs_sparsed_volumes:
            self._create_sparsed_file(volume_path, volume_size)
        else:
            self._create_regular_file(volume_path, volume_size)

        self._set_rw_permissions_for_all(volume_path)

    def _ensure_shares_mounted(self):
        """Mount locally all XtreemFS shares."""
        for share in self._load_shares_from_config():
            try:
                self._ensure_share_mounted(share)
                self._mounted_shares.append(share)
            except Exception as exc:
                LOG.warning(_('Exception during mounting %s') % (exc,))

        # If no shared was mounted successfully raise an error.
        if not self._mounted_shares:
            raise exception.XtreemfsNoSharesMounted()
        LOG.debug(_('Available shares %r') % self._mounted_shares)

    def _load_shares_from_config(self):
        """Read and return shares from configuration file."""
        with open(self.configuration.xtreemfs_shares_config) as conf:
            shares = [share.strip() for share in conf
                      if share and not share.startswith('#')]
        return shares

    def _ensure_share_mounted(self, xtreemfs_share):
        """Mount XtreemFS share."""
        mount_path = self._get_mount_point_for_share(xtreemfs_share)
        self._mount_xtreemfs(xtreemfs_share, mount_path)

    def _find_share(self, volume_size):
        """Find XtreemFS share that could host given volume size.

        Current implementation looks for share with the greatest available
        capacity.

        :param volume_size: int size in GB
        """
        biggest_size = 0
        biggest_share = None

        for xtreemfs_share in self._mounted_shares:
            capacity = self._get_available_capacity(xtreemfs_share)
            if capacity > biggest_size:
                biggest_share = xtreemfs_share
                biggest_size = capacity

        # Raise an error if volume size can not be hosted in any share.
        if volume_size * 1024 * 1024 * 1024 > biggest_size:
            raise exception.XtreemfsNoSuitableShareFound(
                volume_size=volume_size)
        return biggest_share

    def _get_mount_point_for_share(self, xtreemfs_share):
        """Return local mount point for given share.

        :param xtreemfs_share: example 10.123.0.10:/var/xtreemfs
        """
        return os.path.join(self.configuration.xtreemfs_mount_point_base,
                            self._get_hash_str(xtreemfs_share))

    def _get_available_capacity(self, xtreemfs_share):
        """Calculate available space on the XtreemFS share.

        :param xtreemfs_share: example 10.123.0.10:/var/xtreemfs
        """
        mount_point = self._get_mount_point_for_share(xtreemfs_share)
        out, _ = self._execute('df', '--portability', '--block-size', '1',
                               mount_point)
        out = out.splitlines()[1]

        available = 0
        if self.configuration.xtreemfs_disk_util == 'df':
            available = int(out.split()[3])
        else:
            size = int(out.split()[1])
            out, _ = self._execute('du', '-sb', '--apparent-size',
                                   '--exclude', '*snapshot*', mount_point)
            used = int(out.split()[0])
            available = size - used
        return available

    def _mount_xtreemfs(self, xtreemfs_share, mount_path):
        """Mount XtreemFS share to mount path."""
        self._execute('mkdir', '-p', mount_path)

        self._execute('mount.xtreemfs', '-o', 'allow_other',
                      xtreemfs_share, mount_path, run_as_root=True)
