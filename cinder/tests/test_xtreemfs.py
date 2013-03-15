# -*- coding: utf-8 *-*
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

import os
import errno
import random
import __builtin__

import mox
from mox import IsA, IgnoreArg

from cinder.volume import configuration
from cinder.volume.drivers import xtreemfs
from cinder import test, context, exception


MAX = 2 << 20  # Dummy max size in Byte.


def _get_dummy_volume():
    "Get dummy volume to run the test with."
    return {
        "provider_location": "127.0.0.1:/mnt",
        "name": "dummy",
        "size": 10
    }


class XtreemFsDriverTestCase(test.TestCase):
    "Test Case for Xtreemfs driver."

    available_shares = ["1:/sdc", "2:/sdc"]
    mount_point = "/mnt/"
    shares_config_filepath = "/etc/cinder/shares.txt"

    def setUp(self):
        self._mocker = mox.Mox()
        self._config = mox.MockObject(configuration.Configuration)
        self._config.xtreemfs_shares_config = self.shares_config_filepath
        self._config.xtreemfs_disk_util = "df"
        self._config.xtreemfs_sparsed_volumes = True
        self._config.xtreemfs_mount_point_base = self.mount_point
        self._driver = xtreemfs.XtreemfsDriver(configuration=self._config)

    def tearDown(self):
        self._mocker.UnsetStubs()

    def test_setup_without_shares_config_file(self):
        "Setup should raise an error if shares config wasn't set."
        self._config.xtreemfs_shares_config = ""
        self.assertRaises(
            exception.XtreemfsException,
            self._driver.do_setup,
            IsA(context.RequestContext)
        )

    def test_setup_with_unexisting_shares_config_file(self):
        "Setup should raise an error if shares config path doesn't exist."
        # Mock checking for existing config file to return False.
        self._mocker.StubOutWithMock(os.path, "exists")
        os.path.exists(self.shares_config_filepath).AndReturn(False)
        self._mocker.ReplayAll()
        # Should raise an error if path doesn't exist.
        self.assertRaises(
            exception.XtreemfsException,
            self._driver.do_setup,
            IsA(context.RequestContext)
        )
        self._mocker.VerifyAll()

    def test_setup_without_client_installed(self):
        "Setup should raise an error if xtreemfs client wasn't installed."
        # Mock checking for existing config file to return True.
        self._mocker.StubOutWithMock(os.path, "exists")
        os.path.exists(self.shares_config_filepath).AndReturn(True)
        # Mock execute method to fail locating the mount.xtreemfs command.
        self._mocker.StubOutWithMock(self._driver, '_execute')
        self._driver._execute('mount.xtreemfs', check_exit_code=False).\
                     AndRaise(
                         OSError(errno.ENOENT, 'No such file or directory'))
        self._mocker.ReplayAll()
        self.assertRaises(
            exception.XtreemfsException,
            self._driver.do_setup,
            IsA(context.RequestContext)
        )
        self._mocker.VerifyAll()

    def test_find_shares_without_shares(self):
        "Find share should raise an error if no share was given."
        self._driver._mounted_shares = []
        self.assertRaises(
            exception.XtreemfsNoSharesMounted,
            self._driver._find_share,
            1
        )

    def test_find_shares_without_enough_space(self):
        "Find share should raise an error if no share can hold size given."
        # Add dummy shares and mock the capacity to return 0 for each one.
        self._driver._mounted_shares = self.available_shares
        self._mocker.StubOutWithMock(self._driver, "_get_available_capacity")
        for share in self._driver._mounted_shares:
            self._driver._get_available_capacity(share).AndReturn(0)
        self._mocker.ReplayAll()
        self.assertRaises(
            exception.XtreemfsNoSuitableShareFound,
            self._driver._find_share,
            1
        )
        self._mocker.VerifyAll()

    def test_read_shares_from_config(self):
        "Test read shares from the shares config file."
        # Mock Python builtin's open function to read shares config file.
        self._mocker.StubOutWithMock(__builtin__, "open")
        __builtin__.open(self.shares_config_filepath).AndReturn(
            self.available_shares + ["# ignored"]
        )
        self._mocker.ReplayAll()
        self.assertEquals(
            self.available_shares,
            self._driver._load_shares_config()
        )
        self._mocker.VerifyAll()

    def test_get_capacity_with_df(self):
        "Test getting volume capacity using df command."
        share = self.available_shares[0]
        # Mock driver to return dummy mount point.
        self._mocker.StubOutWithMock(
            self._driver, "_get_mount_point_for_share"
        )
        self._driver._get_mount_point_for_share(share) \
                    .AndReturn(self.mount_point)
        # Mock execute command to return dummy result for df command.
        available = random.randint(1, MAX)
        cmd_output = (
            "Filesystem 1K-blocks Used Available Use%% Mounted on\n" +
            "%s <dummy> <dummy> %d <dummy> <dummy>"
        ) % (share, available)
        self._mocker.StubOutWithMock(self._driver, "_execute")
        self._driver._execute(
            "df", "--portability", "--block-size", "1",
            self.mount_point,
            run_as_root=True
        ).AndReturn((cmd_output, None))
        self._mocker.ReplayAll()
        # Check that the returned capacity is the same returned by df command.
        self.assertEquals(
            available,
            self._driver._get_available_capacity(share)
        )
        self._mocker.VerifyAll()

    def test_get_capacity_with_du(self):
        "Test getting volume capacity using du command."
        share = self.available_shares[0]
        # Use du to retrieve volume size.
        self._config.xtreemfs_disk_util = "du"
        # Mock driver to return dummy mount point.
        self._mocker.StubOutWithMock(
            self._driver, "_get_mount_point_for_share"
        )
        self._driver._get_mount_point_for_share(share) \
                    .AndReturn(self.mount_point)
        # Mock execute command to return dummy result for df command.
        total_size, used_size, available_size = random.sample(xrange(MAX), 3)
        cmd_output = (
            "Filesystem 1-blocks Used Available Use%% Mounted on\n" +
            "%s %d %d %d 41%% /mnt"
        ) % (share, total_size, used_size, available_size)
        self._mocker.StubOutWithMock(self._driver, '_execute')
        self._driver._execute(
            "df", "--portability", "--block-size", "1",
            self.mount_point,
            run_as_root=True
        ).AndReturn((cmd_output, None))
        # Mock execute command to return dummy result for du command.
        used = random.randint(1, MAX)
        cmd_output = "%d /mnt" % used
        self._driver._execute(
            "du", "-sb", "--apparent-size", "--exclude", "*snapshot*",
            self.mount_point,
            run_as_root=True
        ).AndReturn((cmd_output, None))

        self._mocker.ReplayAll()
        self.assertEquals(
            total_size - used,
            self._driver._get_available_capacity(share)
        )
        self._mocker.VerifyAll()

    def test_ensure_share_mounted(self):
        "Ensuring a shared mounted volume."
        # Mock getting mounting share to do nothing.
        share = self.available_shares[0]
        # Mock driver to return dummy mount point.
        self._mocker.StubOutWithMock(
            self._driver, '_get_mount_point_for_share'
        )
        self._driver._get_mount_point_for_share(share).\
                     AndReturn(self.mount_point)
        # Mock mounting volume to do nothing.
        self._mocker.StubOutWithMock(self._driver, '_mount_xtreemfs')
        self._driver._mount_xtreemfs(share, self.mount_point, ensure=True)

        self._mocker.ReplayAll()
        self._driver._ensure_share_mounted(share)
        self._mocker.VerifyAll()

    def test_ensure_shares_save(self):
        "Check that ensure mounted shares save loaded share."
        # Mock loading shared config to return dummy shares.
        self._mocker.StubOutWithMock(self._driver, '_load_shares_config')
        self._driver._load_shares_config().AndReturn(self.available_shares)
        # Mock ensuring single share to do nothing.
        self._mocker.StubOutWithMock(self._driver, '_ensure_share_mounted')
        for share in self.available_shares:
            self._driver._ensure_share_mounted(share)
        # Check that all shares was loaded.
        self._mocker.ReplayAll()
        self._driver._ensure_shares_mounted()
        self.assertListEqual(
            self._driver._mounted_shares, self.available_shares
        )
        self._mocker.VerifyAll()

    def test_ensure_shares_save_only_correct_share(self):
        "Check that only shares that don't raise any error get loaded."
        # Mock loading shared config to return dummy shares.
        self._mocker.StubOutWithMock(self._driver, '_load_shares_config')
        self._driver._load_shares_config().AndReturn(self.available_shares)
        # Mock ensuring single share to succeed all except the first one.
        self._mocker.StubOutWithMock(self._driver, '_ensure_share_mounted')
        self._driver._ensure_share_mounted(self.available_shares[0])\
                    .AndRaise(Exception())
        for share in self.available_shares[1:]:
            self._driver._ensure_share_mounted(share)

        self._mocker.ReplayAll()
        self._driver._ensure_shares_mounted()
        self.assertListEqual(
            self._driver._mounted_shares, self.available_shares[1:]
        )
        self._mocker.VerifyAll()

    def test_create_volume(self):
        "The method that create a volume should return volume's location."
        volume = _get_dummy_volume()
         # Mock ensuring mounted volume to do nothing.
        self._mocker.StubOutWithMock(self._driver, "_ensure_shares_mounted")
        self._driver._ensure_shares_mounted()
        # Mock creating a volume to do nothing.
        self._mocker.StubOutWithMock(self._driver, "_do_create_volume")
        self._driver._do_create_volume(volume)
        # Mock finding share to return dummy share.
        self._mocker.StubOutWithMock(self._driver, "_find_share")
        self._driver._find_share(volume["size"]). \
                     AndReturn(volume["provider_location"])

        self._mocker.ReplayAll()
        self.assertDictEqual(
            {'provider_location': volume["provider_location"]},
            self._driver.create_volume(volume)
        )
        self._mocker.VerifyAll()

    def test_create_sparse_volume(self):
        "Test creating a sparsed volume."
        volume = _get_dummy_volume()
        # Mock method to create sparsed file to do nothing.
        self._mocker.StubOutWithMock(self._driver, "_create_sparsed_file")
        self._driver._create_sparsed_file(IgnoreArg(), IgnoreArg())
        # Mock method to set permission to do nothing.
        self._mocker.StubOutWithMock(
            self._driver, "_set_rw_permissions_for_all")
        self._driver._set_rw_permissions_for_all(IgnoreArg())

        self._mocker.ReplayAll()
        self._driver._do_create_volume(volume)
        self._mocker.VerifyAll()

    def test_create_regular_volume(self):
        "Test creating a regular volume."
        self._config.xtreemfs_sparsed_volumes = False
        volume = _get_dummy_volume()
        # Mock method to create regular file to do nothing.
        self._mocker.StubOutWithMock(self._driver, "_create_regular_file")
        self._driver._create_regular_file(IgnoreArg(), IgnoreArg())
        # Mock method to set permission to do nothing.
        self._mocker.StubOutWithMock(
            self._driver, "_set_rw_permissions_for_all")
        self._driver._set_rw_permissions_for_all(IgnoreArg())

        self._mocker.ReplayAll()
        self._driver._do_create_volume(volume)
        self._mocker.VerifyAll()

    def test_delete_volume(self):
        "Test deleting a volume."
        volume = _get_dummy_volume()
        volume_path = self._driver._get_mount_point_for_share(
            volume["provider_location"]
        )
        # Mock getting volume local path to return dummy value.
        self._mocker.StubOutWithMock(self._driver, "local_path")
        self._driver.local_path(volume).AndReturn(volume_path)
        # Mock checking if path exist to return True for the dummy volume.
        self._mocker.StubOutWithMock(self._driver, "_path_exists")
        self._driver._path_exists(volume_path).AndReturn(True)
        # Mock removing local volume path to do nothing.
        self._mocker.StubOutWithMock(self._driver, "_execute")
        self._driver._execute("rm", "-f", volume_path, run_as_root=True)
        # Mock mounting a volume.
        self._mocker.StubOutWithMock(self._driver, "_mount_xtreemfs")
        self._driver._mount_xtreemfs(
            volume["provider_location"], volume_path, ensure=True
        )

        self._mocker.ReplayAll()
        self._driver.delete_volume(volume)
        self._mocker.VerifyAll()

    def test_delete_witout_location(self):
        "deleting a volume without location should fail silently."
        volume = _get_dummy_volume()
        volume["provider_location"] = None

        self._driver.delete_volume(volume)

    def test_delete_unexisting_volume(self):
        "deleting a volume that doesn't exist should fail silently."
        volume = _get_dummy_volume()
        volume_path = self._driver._get_mount_point_for_share(
            volume["provider_location"]
        )
        # Mock getting volume loval path tp return dummy value.
        self._mocker.StubOutWithMock(self._driver, "local_path")
        self._driver.local_path(volume).AndReturn(volume_path)
        # Mock checking if path exist to return False.
        self._mocker.StubOutWithMock(self._driver, "_path_exists")
        self._driver._path_exists(volume_path).AndReturn(False)
        # Mock ensure share mounted to do nothing.
        self._mocker.StubOutWithMock(self._driver, "_ensure_share_mounted")
        self._driver._ensure_share_mounted(volume["provider_location"])

        self._mocker.ReplayAll()
        self._driver.delete_volume(volume)
        self._mocker.VerifyAll()

    def test_mount_volume(self):
        "Test mounting a volume."
        share = self.available_shares[0]
        # Mock checking if mounting path exist to return true.
        self._mocker.StubOutWithMock(self._driver, "_path_exists")
        self._driver._path_exists(self.mount_point).AndReturn(True)
        # Mock running xtreemfs mount command to do nothing.
        self._mocker.StubOutWithMock(self._driver, "_execute")
        self._driver._execute(
            "mount.xtreemfs", share, self.mount_point, run_as_root=True
        )

        self._mocker.ReplayAll()
        self._driver._mount_xtreemfs(share, self.mount_point)
        self._mocker.VerifyAll()

    def test_mount_volume_failure(self):
        "Failure in mounting should be silent if ensure is True."
        share = self.available_shares[0]
        # Mock checking if path exist with a dummy value.
        self._mocker.StubOutWithMock(self._driver, "_path_exists")
        self._driver._path_exists(self.mount_point).AndReturn(True)
        # Mock running xtreemfs mount command to raise an error.
        self._mocker.StubOutWithMock(self._driver, "_execute")
        self._driver._execute(
            "mount.xtreemfs", share, self.mount_point, run_as_root=True
        ).AndRaise(exception.ProcessExecutionError(stderr="already mounted"))

        self._mocker.ReplayAll()
        self._driver._mount_xtreemfs(share, self.mount_point, ensure=True)
        self._mocker.VerifyAll()

    def test_mount_volume_failure_silence(self):
        "Failure in mounting should be raised if ensure is True."
        share = self.available_shares[0]
        # Mock checking if path exist with a dummy value.
        self._mocker.StubOutWithMock(self._driver, "_path_exists")
        self._driver._path_exists(self.mount_point).AndReturn(True)
        # Mock running xtreemfs mount command to raise an error.
        self._mocker.StubOutWithMock(self._driver, "_execute")
        self._driver._execute(
            "mount.xtreemfs", share, self.mount_point, run_as_root=True
        ).AndRaise(exception.ProcessExecutionError(stderr="already mounted"))

        self._mocker.ReplayAll()
        self.assertRaises(
            exception.ProcessExecutionError, self._driver._mount_xtreemfs,
            share, self.mount_point, ensure=False
        )
        self._mocker.VerifyAll()

    def test_mount_volume_with_unexisting_mounting_point(self):
        "Mounting a volume should create mounting point if it doesn't exist."
        share = self.available_shares[0]
        # Mock checking if path exist with a dummy value.
        self._mocker.StubOutWithMock(self._driver, "_path_exists")
        self._driver._path_exists(self.mount_point).AndReturn(False)
        # Mock running mkdir for mounting point.
        self._mocker.StubOutWithMock(self._driver, "_execute")
        self._driver._execute("mkdir", "-p", self.mount_point)
        # Mock running xtreemfs mount.
        self._driver._execute(
            "mount.xtreemfs", share, self.mount_point, run_as_root=True
        )

        self._mocker.ReplayAll()
        self._driver._mount_xtreemfs(share, self.mount_point)
        self._mocker.VerifyAll()
