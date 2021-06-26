#coding:utf8
import paramiko,yaml,logging#Must install paramiko,yaml
host_ip = "10.121.133.101" #Any storage node IP
username = "root"  #Host user name
passwd = "root"    #Host user password
pool = "ucpool"    #Ceph storage pool name

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
    :return: execute Cli output
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
    :param client:ssh client
    :return: all snapshots of the rbd
    """
    cmd = r"rbd snap ls " + pool + "/" + rbd_id + "|grep -v NAME|awk '{print $2}'"
    snapshot = RunCMD(cmd,client)
    return snapshot

def GetChildrenBySnap(rbd_id,snap_id,client):
    """

    :param snap_id:
    :param client: ssh client
    :return: children of the snapshot
    """
    cmd = "rbd children " + pool + "/" + rbd_id + "@" + snap_id
    children = list()
    try:
        all_children = str(RunCMD(cmd, client)).splitlines()#split("/")[1]
        for child in all_children:
            children.append(child.split("/")[1])
    except IndexError:
        LogOutput("There is no clone children from the " + snap_id + "of the volume "+ rbd_id )
    return children

def GetRBDChildren(rbd_id,all_chilren_rbd,client):
    """

    :param rbd_id:
    :param all_chilren_rbd: storage all children
    :param client: ssh client
    :return: storage all children
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
                for child in children:
                    #children = str(children).replace("\n", "")
                    child = str(child).replace("\n", "")
                    children_in_a_rbd.append(child)
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
    :param client:ssh client
    :return: top parent rbd id
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

def GetCloneChain(rbd_id,client):
    """

    :param rbd_id:
    :param client:ssh client
    :return: type:list,Complete clone chain
    """

    chilren_rbd = list()
    top_parent = GetTopParent(rbd_id, client)
    all_clone_chain = GetRBDChildren(top_parent, chilren_rbd, client)
    return all_clone_chain


def WriteFile(rbd_clone_tree):
    """

    :param rbd_clone_tree: type:dict
    :return: write clone dict into yaml file
    """
    with open('rbd_clone_tree.yaml', 'w+', encoding="utf-8") as f:
        yaml.dump(rbd_clone_tree, f)


def CreateTree(tree,all_clone_chain):
    """

    :param tree: init clone tree,type:dict
    :param all_clone_chain: type:list,Complete clone chain
    :return: clone tree
    """
    for key,value in tree.items():
        for i in range(0,len(value)):
            for one in all_clone_chain[1:]:
                if isinstance(value[i],dict):
                    CreateTree(value[i],all_clone_chain)
                else:
                    if value[i] in list(one.keys()):
                        tree[key][i] = {value[i]:one.get(value[i])}
    return tree

def main(rbd_id):
    """
    main function,program entry
    :param rbd_id:
    :return: None
    """
    LogOutput("Step 1:Ceate ssh client and SSH login!")
    client = CLIClient()
    LogOutput("Step 2:Get top parent rbd of the " + rbd_id)
    top_parent = GetTopParent(rbd_id,client)
    LogOutput("Step 3:Get complete clone relationship of the " + rbd_id)
    all_clone_chain = GetCloneChain(top_parent,client)
    LogOutput("Step 4:Create clone tree of the " + rbd_id + ",and write clone tree into yaml file!")
    try:
        init_tree = all_clone_chain[0]
        tree = CreateTree(init_tree,all_clone_chain)
        WriteFile(tree)
    except IndexError:
        LogOutput(rbd_id + "has not clone rbd!") #if rbd has not clone rbd,don't write yaml file
    LogOutput("Step 5:Close ssh connection!")
    client.close()

if __name__=="__main__":
    main("B_1")
