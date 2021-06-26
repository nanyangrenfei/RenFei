#coding:utf8
import paramiko,logging #Must install paramiko
host_ip = "10.121.133.101" #Any storage node IP
username = "root"
passwd = "root"
pool = "ucpool"
flatten_rbd_file_path = "/home/rbd_list" #Write all rbd into the rbd_list

def LogOutput(mes):
    """

    :param mes: Log message!
    :return: None
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    logger.info(mes)

def CLIClient():
    """

    :return:  Get SSH client
    """
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(host_ip, 22, username, passwd, timeout=120)
    return ssh_client


def RunCMD(cmd,client):
    """
    :param cmd: shell cli command
    :param client: SSH client
    :return:
    """
    LogOutput("Running CLi: "+cmd)
    std_in, std_out, std_err = client.exec_command(cmd)
    output = std_out.read().decode()
    return output


def GetRBDParent(rbd_id,all_parent_rbd,client):
    """

    :param rbd_id: rbd id to be deleted!
    :param all_parent_rbd:List,recored all parent rbd
    :param client:SSH client
    :return:None
    """


    cmd = "rbd info " + pool + "/" + rbd_id + "|grep parent"
    rbd_parent = RunCMD(cmd,client)
    if not rbd_parent:
        pass
    else:
        LogOutput(rbd_id + " has parent:" + rbd_parent.strip())
        parent_rbd_id = rbd_parent.split(":")[1].split("@")[0].split("/")[1]
        all_parent_rbd.append(parent_rbd_id)
        GetRBDParent(parent_rbd_id,all_parent_rbd,client)


def GetRBDSnapshot(rbd_id,client):
    """

    :param rbd_id:
    :param client:
    :return:
    """
    cmd = r"rbd snap ls " + pool + "/" + rbd_id + "|grep -v NAME|awk '{print $2}'"
    snapshot = RunCMD(cmd,client)
    return snapshot

def GetChildrenBySnap(rbd_id,snap_id,client):
    """

    :param snap_id:
    :param client:
    :return:
    """
    cmd = "rbd children " + pool + "/" + rbd_id + "@" + snap_id
    children = None
    try:
        children = RunCMD(cmd, client).split("/")[1]
    except IndexError:
        LogOutput("There is no clone children from the " + snap_id + "of the volume "+ rbd_id )
    return children

def GetRBDChildren(rbd_id,all_chilren_rbd,client):
    """

    :param rbd_id:
    :param all_chilren_rbd:
    :param client:
    :return:
    """
    snapshot = GetRBDSnapshot(rbd_id, client).splitlines()
    children_in_a_rbd = list()
    children_dict = dict()
    if snapshot:
        LogOutput(str(snapshot) + " is got snapshot from " + rbd_id)
        for snap in snapshot:
            children = GetChildrenBySnap(rbd_id, snap, client)
            if not children:
                pass
            else:
                children = str(children).replace("\n", "")
                children_in_a_rbd.append(children)
                children_dict[rbd_id] = children_in_a_rbd
        LogOutput("Clone rbds " + str(children_in_a_rbd) + " base on " + rbd_id)
        all_chilren_rbd.append(children_dict)
        for child in children_in_a_rbd:
            GetRBDChildren(child, all_chilren_rbd, client)
    else:
        LogOutput(rbd_id + " is the bottom clone rbd")


    return all_chilren_rbd

def GetTopParent(rbd_id,client):
    """

    :param rbd_id:
    :param client:
    :return:
    """

    all_parent_rbd = list()
    top_parent = None
    GetRBDParent(rbd_id, all_parent_rbd, client)

    if all_parent_rbd:
        top_parent = all_parent_rbd[-1]
        LogOutput("Top parent of the RBD '" + rbd_id + "' is " + top_parent)
    else:
        top_parent = rbd_id
        LogOutput(rbd_id + " itself  is at the top!")
    return top_parent

def ExecuteFlatten(rbd_id,client):
    """

    :param rbd_id: rbd id to be deleted!
    :param client: SSH client
    :return: None
    """
    chilren_rbd = list()
    top_parent = GetTopParent(rbd_id, client)
    all_chilren_rbd = GetRBDChildren(top_parent, chilren_rbd, client)
    all_chilren_rbd_reverse = all_chilren_rbd[::-1]
    if all_chilren_rbd_reverse:
        for chilren in all_chilren_rbd_reverse:
            for child in chilren.values():
                for vol in child:
                    LogOutput(vol + "will be flatten")
                    cmd = "rbd flatten " + pool + "/" + vol
                    print(cmd)
                    RunCMD(cmd,client)


def FlattenAllRBD():
    """
    Read rbd id  to be deleted from the file,batch execute!
    :return: None
    """
    client = CLIClient()
    cmd = "cat " + flatten_rbd_file_path
    all_rbd = RunCMD(cmd,client)
    all_rbd_list = all_rbd.splitlines()
    for volume in all_rbd_list:
        ExecuteFlatten(volume,client)
    client.close()

def DeleteRBD():
    client = CLIClient()
    cmd = "cat " + flatten_rbd_file_path
    all_rbd = RunCMD(cmd, client)
    all_rbd_list = all_rbd.splitlines()
    for volume in all_rbd_list:
        snapshot = GetRBDSnapshot(volume, client).splitlines()
        if snapshot:
            for snap in snapshot:
                cmd = "rbd snap unprotect " + pool + "/" + volume + "@" + snap
                RunCMD(cmd,client)
                cmd = "rbd snap rm " + pool + "/" + volume + "@" + snap
                RunCMD(cmd, client)
        LogOutput(volume + " will be deleted!")
        cmd = "rbd  rm " + pool + "/" + volume
        RunCMD(cmd, client)
    client.close()



if __name__=="__main__":
    FlattenAllRBD()
    DeleteRBD()
