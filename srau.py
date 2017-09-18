#!/usr/bin/env python
"""
Author: Samuel Gass
Email: adahn6@gmail.com
"""

import argparse
import atexit
from datetime import datetime

import pywbem
from lxml import etree as ET
from pyVim.connect import Disconnect, SmartConnectNoSSL
from pyVmomi import vim

SMIS_IP = "http://192.168.1.1"
SMIS_PORT = "5988"
SMIS_USER = "admin"
SMIS_PASS = "#1Password"

VCENTER_IP = "192.168.1.2"
VCENTER_PORT = "443"
VCENTER_USER = "administrator@vsphere.local"
VCENTER_PASS = "password"

COPY_TYPE = "SNAPVX"
COPY_MODE = "NOCOPY"

FILENAME = "EmcSrdfSraTestFailoverConfig.xml"
FILENAME_MASKING_INFO = "EmcSrdfSraMaskingInfo.xml"
FILENAME_ALL_DEVICES = "AllMaskingDevices.txt"


def parse_vsphere_content(vsphere_conn, debug=False):
    """Return a dictionary with the relevant content from vSphere
    """
    scsi_luns = []
    extent_objs = []
    disks = []

    content = vsphere_conn.RetrieveContent()
    children = content.rootFolder.childEntity
    for child in children:
        clusters = child.hostFolder.childEntity
        for cluster in clusters:
            for host in cluster.host:
                scsi_luns.extend(host.config.storageDevice.scsiLun)
        datastores = child.datastore
        for datastore in datastores:
            if hasattr(datastore.info, 'vmfs'):
                if not datastore.info.vmfs is None:
                    extent_objs.extend(datastore.info.vmfs.extent)
        virtual_machines = recurse_folder(child.vmFolder)
        for virtual_machine in virtual_machines:
            disks.extend(virtual_machine.config.hardware.device)

    content = {}
    content["luns"] = parse_luns(scsi_luns, debug)
    content["extents"] = parse_extents(extent_objs, debug)
    parse_disks(disks, content["extents"], debug)
    return content


def parse_luns(scsi_luns, debug):
    """Parse luns from vSphere by filtering for VMAX WWNs
    """
    luns = []
    for lun in scsi_luns:
        beg = lun.uuid.find("6000097")
        if beg == -1:
            continue
        else:
            lun_wwn = lun.uuid[beg:beg + 32]
            if lun_wwn not in luns:
                luns.append(lun_wwn)
            if debug:
                print "Found lun " + lun_wwn
    return luns


def parse_extents(extent_objs, debug):
    """Return a map of array ID -> extent WWNs
    """
    extents = {}
    for extent in extent_objs:
        extent_id = extent.diskName
        symm = extent_id[12:24]
        if symm in extents.keys():
            extents[symm].append(extent_id[12:])
        else:
            extents[symm] = [extent_id[12:]]
        if debug:
            print "Found extent lun " + extent_id[12:]
    return extents


def parse_disks(disks, extents, debug):
    """Add VM WWNs to extents
    """
    for disk in disks:
        if not disk.backing is None:
            if hasattr(disk.backing, "compatibilityMode"):
                if disk.backing.compatibilityMode == "physicalMode":
                    symm = disk.backing.lunUuid[18:30]
                    if symm in extents.keys():
                        extents[symm].append(disk.backing.lunUuid[18:-12])
                    else:
                        extents[symm] = [disk.backing.lunUuid[18:-12]]
                    if debug:
                        print "Found vm lun " + disk.backing.lunUuid[18:-12]


def recurse_folder(folder):
    """Recursivelye numerate all VMs in the root VM folder
    """
    vms = []
    for child in folder.childEntity:
        if isinstance(child, vim.VirtualMachine):
            vms.append(child)
        elif isinstance(child, vim.Folder):
            vms.extend(recurse_folder(child))
    return vms


def get_symm_system(conn, debug):
    """Get the Symm_StorageSystem name
    """
    systems = conn.EnumerateInstances(
        'Symm_StorageSystem', namespace='root/emc')
    local_sys = ""
    for system in systems:
        if system["EMCLocality"] == 2:
            if local_sys == "":
                local_sys = system
            else:
                if debug:
                    print ("SMI-S sees multiple local VMAX arrays." +
                           "Only 1 local array is supported along with the remote array(s).")
                return None
    array_dt = local_sys["EMCLastSyncTime"].datetime
    curr_time = datetime.now(array_dt.tzinfo)
    time_diff = curr_time - array_dt
    time_diff_in_minutes = divmod(
        time_diff.days * 86400 + time_diff.seconds, 60)[0]
    if time_diff_in_minutes > 15:
        print("***** The SMI-S VMAX array data is stale and requires refreshing. The last sync was "
              + str(time_diff_in_minutes) +
              " minutes ago. The data syncs automatically only "
              + "every hour. Before running the script again, please sync the array manually by "
              + "using the TestSmiProvider binary and executing the refsys command.")
        if not debug:
            quit()
    return local_sys["name"]


def get_rep_volumes(conn, symm, luns, usage, debug):
    """Get all replication volumes
    """
    source_luns = []
    target_luns = []
    rep_source_vols = conn.ExecQuery("DMTF:CQL",
                                     "SELECT * FROM Symm_StorageVolume " +
                                     "WHERE Symm_StorageVolume.SystemName='" + symm +
                                     "' AND Symm_StorageVolume.Usage=9", namespace='root/emc')
    for lun in rep_source_vols:
        if lun["EMCWWN"] in luns:
            source_luns.append(lun)
            if debug:
                print "Found source lun " + lun["DeviceID"] + " on SMI-S provider"

    rep_target_vols = conn.ExecQuery("DMTF:CQL",
                                     "SELECT * FROM Symm_StorageVolume " +
                                     "WHERE Symm_StorageVolume.SystemName='" + symm +
                                     "' AND Symm_StorageVolume.Usage=2", namespace='root/emc')
    for lun in rep_target_vols:
        if usage == "failover":
            if lun["EMCWWN"] in luns:
                target_luns.append(lun)
                if debug:
                    print "Found target lun " + lun["DeviceID"] + " on SMI-S provider"
        elif usage == "masking":
            if lun["EMCWWN"] not in luns:
                target_luns.append(lun)
                if debug:
                    print "Found target lun " + lun["DeviceID"] + " on SMI-S provider"

    luns = {"source": source_luns, "target": target_luns}
    return luns


def find_existing_pairs(conn, symm, luns, source_luns, debug):
    """Filter out existing replication pairs for the relevant devs
    """
    rep_pairs = {}
    rep_targets = conn.ExecQuery("DMTF:CQL",
                                 "SELECT * FROM Symm_StorageVolume " +
                                 "WHERE Symm_StorageVolume.SystemName='" + symm +
                                 "' AND Symm_StorageVolume.Usage=8", namespace='root/emc')

    for lun in rep_targets:
        if lun["EMCWWN"] in luns:
            pairs = conn.Associators(
                lun.path, ResultClass="SE_ReplicaPairView")
            for pair in pairs:
                if pair["SVTargetDeviceID"] == lun["DeviceId"]:
                    if debug:
                        print("Found pairing of source dev " + pair["SVSourceDeviceID"]
                              + " and target device " + pair["SVTargetDeviceID"])
                    for source_lun in source_luns:
                        if source_lun["DeviceID"] == pair["SVSourceDeviceID"]:
                            rep_pairs[pair["SVSourceDeviceID"]
                                     ] = pair["SVTargetDeviceID"]
                            source_luns.remove(source_lun)
                            break
    return rep_pairs


def pair_luns(source_luns, target_luns, rep_pairs, debug):
    """Pair the remaining source and target luns with matching sizes
    """
    if debug:
        print "Using source luns: "
        for lun in source_luns:
            print lun["DeviceID"]
        print "Using target luns: "
        for lun in target_luns:
            print lun["DeviceID"]

    if len(target_luns) < len(source_luns):
        if debug:
            print "Error: Not enough target volumes!!"
        return False

    for source in source_luns:
        curr_size = 0
        curr_dev = None
        for target in target_luns:
            if source["NumberOfBlocks"] == target["NumberOfBlocks"]:
                if curr_size == 0 or curr_size > target["NumberOfBlocks"]:
                    curr_size = target["NumberOfBlocks"]
                    curr_dev = target
                    if debug:
                        print "Found new best target dev " + target["DeviceID"]
        if curr_dev is None:
            if debug:
                print "Error: No dev large enough for source device " + source["DeviceID"]
            return False
        else:
            rep_pairs[source["DeviceID"]] = curr_dev["DeviceID"]
            if debug:
                print "pairing " + source["DeviceID"] + " with " + curr_dev["DeviceID"]
            target_luns.remove(curr_dev)
    return True


def print_xml(symm, rep_pairs, debug):
    """Print XML file
    """
    root = ET.Element("TestFailoverInfo")
    ET.SubElement(root, "Version").text = "6.2"
    copy_info = ET.SubElement(root, "CopyInfo")
    ET.SubElement(copy_info, "ArrayId").text = symm[12:]
    ET.SubElement(copy_info, "CopyType").text = COPY_TYPE
    ET.SubElement(copy_info, "CopyMode").text = COPY_MODE
    ET.SubElement(copy_info, "SavePoolName")

    dev_list = ET.SubElement(copy_info, "DeviceList")
    for source, target in rep_pairs.iteritems():
        dev_pair = ET.SubElement(dev_list, "DevicePair")
        source = ET.SubElement(dev_pair, "Source").text = source
        target = ET.SubElement(dev_pair, "Target").text = target

    tree = ET.ElementTree(root)
    tree.write(FILENAME, pretty_print=True, encoding="ISO-8859-1")
    print "***** File " + FILENAME + " generated successfully."


def print_masking_info(symm, storage_group, rep_pairs, debug):
    """Print XML file for masking info
    """
    root = ET.Element("DeviceMaskingInfo")
    ET.SubElement(root, "Version").text = "6.2"
    mask_view_list = ET.SubElement(root, "MaskViewList")
    mask_view = ET.SubElement(mask_view_list, "MaskView")
    ET.SubElement(mask_view, "ArrayId").text = symm[12:]
    ET.SubElement(mask_view, "StorageGroup").text = storage_group
    device_list = ET.SubElement(mask_view, "DeviceList")
    for source, target in rep_pairs.iteritems():
        ET.SubElement(device_list, "Device").text = target

    tree = ET.ElementTree(root)
    tree.write(FILENAME_MASKING_INFO, pretty_print=True, encoding="ISO-8859-1")
    print "***** File " + FILENAME_MASKING_INFO + " generated successfully."


def print_scratch_devices(luns):
    """Print all matching devices found into extra file
    """
    with open(FILENAME_ALL_DEVICES, "w+") as f:
        print >> f, "Device list: "
        for lun in luns:
            print >> f, lun["DeviceID"]
    print "***** File " + FILENAME_ALL_DEVICES + " generated successfully."


def filter_used_luns(symm, extents, target_luns, debug):
    """Filter out target luns used by VMs
    """
    extent_devs = []
    if symm[12:] in extents.keys():
        for extent in extents[symm[12:]]:
            dev_id = extent[-10:].decode('hex')
            extent_devs.append(dev_id)
        remove_luns = []
        for lun in target_luns:
            if lun["DeviceID"] in extent_devs:
                remove_luns.append(lun)
        for lun in remove_luns:
            if debug:
                print "Removing extent device " + lun["DeviceID"]
            target_luns.remove(lun)


def get_symm_conn(debug):
    """Return a connection to SMI-S provider
    """
    conn = pywbem.WBEMConnection(SMIS_IP + ":" + SMIS_PORT,
                                 (SMIS_USER, SMIS_PASS),
                                 default_namespace='root/emc',
                                 no_verification=True)
    return conn


def get_vsphere_conn(debug):
    """Return a connection to the vSphere instance
    """
    conn = SmartConnectNoSSL(host=VCENTER_IP,
                             user=VCENTER_USER,
                             pwd=VCENTER_PASS,
                             port=VCENTER_PORT)
    if not conn:
        print("Could not connect to the specified host using specified "
              "username and password")
        return None
    atexit.register(Disconnect, conn)
    return conn


def get_storage_group(conn, source_devs, debug):
    """Return the storage group for the R2 device passed in
    """
    if len(source_devs) == 0:
        print "No R2 devices!!"
        return -1

    storage_group = conn.Associators(
        source_devs[0].path, ResultClass="SE_DeviceMaskingGroup")
    if len(storage_group) == 0:
        print "No associated storage group!!"
        return -1
    return storage_group[0]["ElementName"]


def main():
    """Main function for finding replication pairs and creating XML file
    """
    debug = False
    parser = argparse.ArgumentParser(
        description='Create failover pairing XML file.')
    parser.add_argument('--debug', action='store_true',
                        help='enable debug logging')
    parser.add_argument('--usage', choices=['failover', 'maskinginfo'],
                        help='create either failover or masking info XML file')
    args = parser.parse_args()
    if args.debug:
        debug = True

    vsphere_conn = get_vsphere_conn(debug)
    if vsphere_conn is None:
        print "Error connecting to vSphere system! Exiting script."
        return
    vsphere_content = parse_vsphere_content(vsphere_conn, debug)

    symm_conn = get_symm_conn(debug)
    if symm_conn is None:
        print "Error connecting to SMI-S system! Exiting script."
        return

    symm = get_symm_system(symm_conn, debug)
    if symm is None:
        print "Error with SMI-S system-- too many local arrays. Exiting script"
        return

    if args.usage == "failover":
        luns = get_rep_volumes(
            symm_conn, symm, vsphere_content["luns"], "failover", debug)
        filter_used_luns(
            symm, vsphere_content["extents"], luns["target"], debug)
        rep_pairs = find_existing_pairs(
            symm_conn, symm, vsphere_content["luns"], luns["source"], debug)
        paired = pair_luns(luns["source"], luns["target"], rep_pairs, debug)
        if paired:
            print_xml(symm, rep_pairs, debug)
        else:
            print "Error with pairing! Exiting script."
    if args.usage == "maskinginfo":
        luns = get_rep_volumes(
            symm_conn, symm, vsphere_content["luns"], "masking", debug)
        filter_used_luns(
            symm, vsphere_content["extents"], luns["target"], debug)
        rep_pairs = find_existing_pairs(
            symm_conn, symm, vsphere_content["luns"], luns["source"], debug)
        print_scratch_devices(luns["target"])
        paired = pair_luns(luns["source"], luns["target"], rep_pairs, debug)
        storage_group = get_storage_group(symm_conn, luns["source"], debug)
        if paired:
            print_masking_info(symm, storage_group, rep_pairs, debug)
        else:
            print "Error with pairing! Exiting script."


# Start program
if __name__ == "__main__":
    main()
