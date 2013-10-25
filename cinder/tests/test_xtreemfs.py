# -*- coding: utf-8 -*-
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

import errno
import io
import os

import mock
import mox

from cinder import context
from cinder import exception
from cinder import test
from cinder import utils
from cinder.openstack.common import processutils as putils
from cinder.volume import configuration
from cinder.volume.drivers import xtreemfs


MAX = 2 << 20  # Dummy max size in Byte.


def _get_dummy_volume():
    """Get dummy volume to run the test with."""
    return {
        "provider_location": "127.0.0.1:/mnt",
        "name": "dummy",
        "size": 10
    }


class XtreemFsDriverTestCase(test.TestCase):
    """Test Case for Xtreemfs driver."""

    available_shares = ["1:/sdc", "2:/sdc"]
    mount_point = "/mnt/"
    shares_config_filepath = "/etc/cinder/xtreemfs_shares"

    def setUp(self):
        super(XtreemFsDriverTestCase, self).setUp()
        self._mocker = mox.Mox()
        self._config = mox.MockObject(configuration.Configuration)
        self._config.xtreemfs_shares_config = self.shares_config_filepath
        self._config.xtreemfs_sparsed_volumes = True
        self._config.xtreemfs_mount_point_base = self.mount_point

        self._driver = xtreemfs.XtreemfsDriver(configuration=self._config)

    def tearDown(self):
        super(XtreemFsDriverTestCase, self).tearDown()
        self._mocker.UnsetStubs()

    def test_setup_mount_shares(self):
        """Check that do_setup mount all loaded share."""
        # Mock check path exist.
        self._mocker.StubOutWithMock(os.path, 'exists')
        os.path.exists(self.shares_config_filepath).AndReturn(True)
        # Mock loading shared config to return dummy shares.
        self._mocker.StubOutWithMock(self._driver, '_load_shares_from_config')
        self._driver._load_shares_from_config().AndReturn(
            self.available_shares)
        # Mock checking for xtreemfs binary to succeed.
        self._mocker.StubOutWithMock(self._driver, '_execute')
        self._driver._execute('mount.xtreemfs', check_exit_code=False,
                              run_as_root=True)
        # Mock ensuring single share to do nothing.
        self._mocker.StubOutWithMock(self._driver, '_mount_xtreemfs')
        for share in self.available_shares:
            self._driver._mount_xtreemfs(share)
        # Check that all shares was loaded.
        self._mocker.ReplayAll()
        self._driver.do_setup(mox.IsA(context.RequestContext))
        self.assertEqual(
            self._driver._mounted_shares, self.available_shares
        )
        self._mocker.VerifyAll()

    def test_setup_save_only_correct_share(self):
        """Check that only shares that don't raise any error get loaded."""
        # Mock check path exist.
        self._mocker.StubOutWithMock(os.path, 'exists')
        os.path.exists(self.shares_config_filepath).AndReturn(True)
        # Mock loading shared config to return dummy shares.
        self._mocker.StubOutWithMock(self._driver, '_load_shares_from_config')
        self._driver._load_shares_from_config().AndReturn(
            self.available_shares)
        # Mock checking for xtreemfs binary to succeed.
        self._mocker.StubOutWithMock(self._driver, '_execute')
        self._driver._execute('mount.xtreemfs', check_exit_code=False,
                              run_as_root=True)
        # Mock ensuring single share to succeed all except the first one.
        self._mocker.StubOutWithMock(self._driver, '_mount_xtreemfs')
        self._driver._mount_xtreemfs(self.available_shares[0])\
                    .AndRaise(Exception())
        for share in self.available_shares[1:]:
            self._driver._mount_xtreemfs(share)

        self._mocker.ReplayAll()
        self._driver.do_setup(mox.IsA(context.RequestContext))
        self.assertEqual(
            self._driver._mounted_shares, self.available_shares[1:]
        )
        self._mocker.VerifyAll()

    def test_setup_without_shares_config_file(self):
        """Setup should raise an error if shares config wasn't set."""
        self._config.xtreemfs_shares_config = ""
        self.assertRaises(
            exception.XtreemfsException,
            self._driver.do_setup,
            mox.IsA(context.RequestContext)
        )

    def test_setup_with_unexisting_shares_config_file(self):
        """Setup should raise an error if shares config path doesn't exist."""
        # Mock checking for existing config file to return False.
        self._mocker.StubOutWithMock(os.path, "exists")
        os.path.exists(self.shares_config_filepath).AndReturn(False)
        self._mocker.ReplayAll()
        # Should raise an error if path doesn't exist.
        self.assertRaises(
            exception.XtreemfsException,
            self._driver.do_setup,
            mox.IsA(context.RequestContext)
        )
        self._mocker.VerifyAll()

    def test_setup_without_client_installed(self):
        """Setup should raise an error if xtreemfs client wasn't installed."""
        # Mock checking for existing config file to return True.
        self._mocker.StubOutWithMock(os.path, "exists")
        os.path.exists(self.shares_config_filepath).AndReturn(True)
        # Mock execute method to fail locating the mount.xtreemfs command.
        self._mocker.StubOutWithMock(self._driver, '_execute')
        call = self._driver._execute('mount.xtreemfs', check_exit_code=False,
                                     run_as_root=True)
        call.AndRaise(OSError(errno.ENOENT, 'No such file or directory'))

        self._mocker.ReplayAll()
        self.assertRaises(
            exception.XtreemfsException,
            self._driver.do_setup,
            mox.IsA(context.RequestContext)
        )
        self._mocker.VerifyAll()

    def test_setup_with_no_mounted_share(self):
        """Setup should raise an error if no mounted shared was found."""
        # Mock checking for existing config file to return True.
        self._mocker.StubOutWithMock(os.path, "exists")
        os.path.exists(self.shares_config_filepath).AndReturn(True)
        # Mock loading shared config to return empty list.
        self._mocker.StubOutWithMock(self._driver, '_load_shares_from_config')
        self._driver._load_shares_from_config().AndReturn([])
        # Mock execute method to not fail locating the xtreemfs binary.
        self._mocker.StubOutWithMock(self._driver, '_execute')
        self._driver._execute('mount.xtreemfs', check_exit_code=False,
                              run_as_root=True)

        # Set mounted shared to empty.
        self._driver._mounted_shares = []

        self._mocker.ReplayAll()
        self.assertRaises(
            exception.XtreemfsNoSharesMounted,
            self._driver.do_setup,
            mox.IsA(context.RequestContext)
        )
        self._mocker.VerifyAll()

    def test_find_shares_without_shares(self):
        """Find share should raise an error if no share was given."""
        self._driver._mounted_shares = []
        self.assertRaises(
            exception.XtreemfsNoSuitableShareFound,
            self._driver._find_share,
            1
        )

    def test_find_shares_without_enough_space(self):
        """Find share should raise an error if no share can hold size given."""
        # Add dummy shares and mock the capacity to return 0 for each one.
        self._driver._mounted_shares = self.available_shares
        self._mocker.StubOutWithMock(self._driver, "_get_capacity_info")
        for share in self._driver._mounted_shares:
            self._driver._get_capacity_info(share).AndReturn((0, 0, 0))
        self._mocker.ReplayAll()
        self.assertRaises(
            exception.XtreemfsNoSuitableShareFound,
            self._driver._find_share,
            1
        )
        self._mocker.VerifyAll()

    def test_read_shares_from_config(self):
        """Test read shares from the shares config file."""
        # Mock Python builtin's open function to read shares config file.
        conf_file = io.StringIO(u'\n'.join(
            self.available_shares + ["# ignored"]
        ))
        with mock.patch("__builtin__.open", return_value=conf_file):
            self._mocker.ReplayAll()
            self.assertEquals(
                self.available_shares,
                self._driver._load_shares_from_config()
            )
            self._mocker.VerifyAll()

    def test_get_volume_capacity(self):
        """Test getting volume capacity."""
        share = self.available_shares[0]
        # Mock driver to return dummy mount point.
        self._mocker.StubOutWithMock(
            self._driver, "_get_mount_point_for_share"
        )
        self._driver._get_mount_point_for_share(share) \
                    .AndReturn(self.mount_point)

        self._mocker.StubOutWithMock(self._driver, '_execute')
        # Mock "du" command.
        total_size = 2620544
        available = 2129984
        stat_output = '1 %d %d' % (total_size, available)

        self._driver._execute(
            'stat', '-f', '-c', '%S %b %a',
            self.mount_point,
            run_as_root=True
        ).AndReturn((stat_output, None))

        # Mock "du" command.
        used = 490560
        du_output = '%d /mnt' % used

        self._driver._execute(
            'du', '-sb', '--apparent-size',
            '--exclude', '*snapshot*',
            self.mount_point,
            run_as_root=True
        ).AndReturn((du_output, None))

        self._mocker.ReplayAll()
        # Check that the returned capacity is the same returned by df command.
        self.assertEquals(
            (total_size, available, used),
            self._driver._get_capacity_info(share)
        )
        self._mocker.VerifyAll()

    def test_create_volume(self):
        """The method that create a volume should return volume's location."""
        volume = _get_dummy_volume()
        # Mock creating a volume to do nothing.
        self._mocker.StubOutWithMock(self._driver, "_do_create_volume")
        self._driver._do_create_volume(volume)
        # Mock finding share to return dummy share.
        self._mocker.StubOutWithMock(self._driver, "_find_share")
        call = self._driver._find_share(volume["size"])
        call.AndReturn(volume["provider_location"])

        self._mocker.ReplayAll()
        self.assertEqual(
            {'provider_location': volume["provider_location"]},
            self._driver.create_volume(volume)
        )
        self._mocker.VerifyAll()

    def test_create_sparse_volume(self):
        """Test creating a sparsed volume."""
        volume = _get_dummy_volume()
        # Mock method to create sparsed file to do nothing.
        self._mocker.StubOutWithMock(self._driver, "_create_sparsed_file")
        self._driver._create_sparsed_file(mox.IgnoreArg(), mox.IgnoreArg())

        self._mocker.ReplayAll()
        self._driver._do_create_volume(volume)
        self._mocker.VerifyAll()

    def test_create_regular_volume(self):
        """Test creating a regular volume."""
        self._config.xtreemfs_sparsed_volumes = False
        volume = _get_dummy_volume()
        # Mock method to create regular file to do nothing.
        self._mocker.StubOutWithMock(self._driver, "_create_regular_file")
        self._driver._create_regular_file(mox.IgnoreArg(), mox.IgnoreArg())
        # Mock method to set permission to do nothing.

        self._mocker.ReplayAll()
        self._driver._do_create_volume(volume)
        self._mocker.VerifyAll()

    def test_delete_volume(self):
        """Test deleting a volume."""
        volume = _get_dummy_volume()
        volume_path = self._driver._get_mount_point_for_share(
            volume["provider_location"]
        )
        # Mock getting volume local path to return dummy value.
        self._mocker.StubOutWithMock(self._driver, "local_path")
        self._driver.local_path(volume).AndReturn(volume_path)
        # Mock removing local volume path to do nothing.
        self._mocker.StubOutWithMock(self._driver, "_execute")
        self._driver._execute("rm", "-f", volume_path)

        self._mocker.ReplayAll()
        self._driver.delete_volume(volume)
        self._mocker.VerifyAll()

    def test_delete_witout_location(self):
        """deleting a volume without location should fail silently."""
        volume = _get_dummy_volume()
        volume["provider_location"] = None

        self._driver.delete_volume(volume)

    def test_delete_unexisting_volume(self):
        """deleting a volume that doesn't exist should fail silently."""
        volume = _get_dummy_volume()
        volume_path = self._driver._get_mount_point_for_share(
            volume["provider_location"]
        )
        # Mock getting volume local path to return dummy value.
        self._mocker.StubOutWithMock(self._driver, "local_path")
        self._driver.local_path(volume).AndReturn(volume_path)

        self._mocker.ReplayAll()
        self._driver.delete_volume(volume)
        self._mocker.VerifyAll()

    def test_mount_volume(self):
        """Test mounting a volume."""
        share = self.available_shares[0]
        mount_path = self._driver._remotefsclient.get_mount_point(share)
        # Mock running xtreemfs mount command to do nothing.
        self._mocker.StubOutWithMock(self._driver._remotefsclient, "_execute")

        self._driver._remotefsclient._execute(
            "mount", check_exit_code=0
        ).AndReturn(("", ""))
        self._driver._remotefsclient._execute(
            "mkdir", "-p", mount_path, check_exit_code=0)
        self._driver._remotefsclient._execute(
            "mount", "-t", "xtreemfs", '-o', 'allow_other',
            share, mount_path, run_as_root=True,
            root_helper=utils.get_root_helper(),  check_exit_code=0
        )

        self._mocker.ReplayAll()
        self._driver._mount_xtreemfs(share)
        self._mocker.VerifyAll()

    def test_mount_volume_failure(self):
        """Failure in mounting should be raised."""
        share = self.available_shares[0]
        mount_path = self._driver._remotefsclient.get_mount_point(share)
        # Mock running xtreemfs mount command to raise an error.
        self._mocker.StubOutWithMock(self._driver._remotefsclient, "_execute")


        self._driver._remotefsclient._execute(
            "mount", check_exit_code=0
        ).AndReturn(("", ""))
        self._driver._remotefsclient._execute(
            "mkdir", "-p", mount_path, check_exit_code=0)
        self._driver._remotefsclient._execute(
            "mount", "-t", "xtreemfs", '-o', 'allow_other',
            share, mount_path, run_as_root=True,
            root_helper=utils.get_root_helper(),  check_exit_code=0
        ).AndRaise(putils.ProcessExecutionError(stderr="already mounted"))

        self._mocker.ReplayAll()
        self.assertRaises(
            putils.ProcessExecutionError, self._driver._mount_xtreemfs,
            share
        )
        self._mocker.VerifyAll()

    def test_mount_volume_with_unexisting_mounting_point(self):
        """Mounting a volume should create mounting point if it doesn't
        exist.
        """
        share = self.available_shares[0]
        mount_path = self._driver._remotefsclient.get_mount_point(share)
        # Mock running mkdir for mounting path.
        self._mocker.StubOutWithMock(self._driver._remotefsclient, "_execute")

        self._driver._remotefsclient._execute(
            "mount", check_exit_code=0
        ).AndReturn(("", ""))
        self._driver._remotefsclient._execute(
            "mkdir", "-p", mount_path, check_exit_code=0)
        self._driver._remotefsclient._execute(
            "mount", "-t", "xtreemfs", '-o', 'allow_other',
            share, mount_path, run_as_root=True,
            root_helper=utils.get_root_helper(),  check_exit_code=0
        )

        self._mocker.ReplayAll()
        self._driver._mount_xtreemfs(share)
        self._mocker.VerifyAll()
