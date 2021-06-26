#coding:utf8
import paramiko
import unittest
import json
import requests
import urllib
import re
import time
import os
from SSHLibrary import SSHLibrary
import sys
sys.getdefaultencoding()


class GetBuild(object):
    def __init__(self):

        self.host = ["10.121.138.102"]
        self.port = 8100
        self.timeout = 30
        self.user = "root"
        self.password = "111111"
        self.base_path = 'http://10.120.16.212/build/ThinkCloud-SDS/release-2.5/'

    def get_build_path(self):
        response = urllib.urlopen(self.base_path)
        html = response.read()
        html = html.decode("utf-8")
        build_name = re.findall(">ThinkCloud-SDS-.+.tar.gz", html)[-1]
        build_name = build_name.replace(">","")
        #print(build_name)
        #print(self.base_path + build_name)
        return self.base_path + build_name,build_name

    def ExecuteCMD(self, host_ip, username,password,CMD_content):
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(host_ip, 22, username, password, timeout=self.timeout)
        std_in, std_out, std_err = ssh_client.exec_command(CMD_content)
        output = std_out.read().decode()
        ssh_client.close()
        return output

    def get_ssh_client(self,host_ip, username,password,timeout=300):
        sshlibary = SSHLibrary()
        sshlibary.open_connection(host_ip, timeout=timeout)
        sshlibary.login(username,password)
        return sshlibary

    def write_and_read(self,sshlibary,command,expect="#",timeout=None):
        sshlibary.write(command)
        output = sshlibary.read_until(expect)
        return output

    def down_build(self,host_ip, username,password):
        ssh_client = self.get_ssh_client(host_ip, username,password)
        output = self.write_and_read(ssh_client,"rm -rf /home/packege/")
        output = self.write_and_read(ssh_client, "mkdir -p /home/packege")
        output = self.write_and_read(ssh_client, "cd /home/packege")
        path, build_name = self.get_build_path()
        output = self.write_and_read(ssh_client, "wget "+path)
        time.sleep(5)
        ssh_client.close_connection()
    def delete_target_node_packege(self,dist_ip,dist_username,dist_password):
        ssh_client = self.get_ssh_client(dist_ip, dist_username, dist_password)
        output = self.write_and_read(ssh_client, "rm -rf /home/packege/")
        output = self.write_and_read(ssh_client, "mkdir -p /home/packege")
        ssh_client.close_connection()

    def clear_env(self,node_ip,node_username,node_password):
        ssh_client = self.get_ssh_client(node_ip, node_username, node_password)
        clear_cmd ="rm /etc/ceph/ceph.* -rf;rm /opt/ceph/* -rf;rm /Ceph/Meta/* -rf;find / -name zabbix | xargs rm -rf;rpm -qa |grep glus |xargs rpm -e"
        output = self.write_and_read(ssh_client, clear_cmd)
        ssh_client.close_connection()
        return output

    def copy_packege_distance_node(self,host_ip, username,password,dist_ip,dist_username,dist_password):
        self.delete_target_node_packege(dist_ip, dist_username, dist_password)
        ssh_client = self.get_ssh_client(host_ip, username, password)
        output = self.write_and_read(ssh_client, "cd /home/packege")
        path, build_name = self.get_build_path()
        copy_command = "scp %s %s@%s:/home/packege/"%(build_name,dist_username,dist_ip)
        print(copy_command)
        output = self.write_and_read(ssh_client, copy_command,expect="password:",timeout=20)
        print(output)
        if "yes/no" in output:
            output = self.write_and_read(ssh_client, "yes", expect="password:")
        if "password:" in output:
            output = self.write_and_read(ssh_client, "root")
        else:
            raise Exception("copy packege failed")

    def install_packege(self,exe_host_info = {},node1_info = {},node2_info = {},node3_info ={},
                        vip = '',netmask = '',filepath = '',clusterscope =''):
        sds_env_list = [node1_info,node2_info,node3_info]
        print(sds_env_list)
        for env in sds_env_list:
            output = self.clear_env(env['nodeip'],env['username'],env['password'])
            print(output)
        ssh_client = self.get_ssh_client(node1_info["nodeip"], node1_info["username"], node1_info["password"],
                                         timeout=7200)
        output = self.write_and_read(ssh_client, "cd /home/packege;tar -zxf  "+filepath)
        install_cmd = "/home/packege/deployment/install.sh -s yes -f 3 --localip %s --node2ip %s --node3ip %s --vip %s --localnodehostname controller-1 " \
                      "--node2hostname controller-2 --node3hostname controller-3 --netmask %s --node2rootpassword %s --node3rootpassword %s --deploymentTarFilePath %s" \
                      " --clusterscope %s"%(node1_info["nodeip"],node2_info["nodeip"],node3_info["nodeip"],vip,netmask,node2_info["password"],node3_info["password"],\
                      filepath,clusterscope)
        print("nohup "+install_cmd+" > /home/install.log 2>&1 &")
        output = self.write_and_read(ssh_client,"ls /home/packege/deployment/")
        if "install.sh" in output:
            output = self.write_and_read(ssh_client, "nohup "+install_cmd+" > /home/install.log 2>&1 &")
            print(output)

    def install_process(self,exe_host_info = {},node1_info = {},node2_info = {},node3_info ={},
                        vip = '',netmask = '',filepath = '',clusterscope =''):
        print(exe_host_info)
        self.down_build(exe_host_info["nodeip"],exe_host_info["username"],exe_host_info["password"])
        self.delete_target_node_packege(node1_info["nodeip"],node1_info["username"],node1_info["password"])
        self.copy_packege_distance_node(exe_host_info["nodeip"],exe_host_info["username"],exe_host_info["password"],
                                        node1_info["nodeip"], node1_info["username"], node1_info["password"])
        self.install_packege(exe_host_info = exe_host_info,node1_info = node1_info,node2_info = node2_info,node3_info =node3_info,
                        vip = vip,netmask = netmask,filepath = filepath,clusterscope =clusterscope)
if __name__=="__main__":
    #path = GetBuild().get_build_path()
    #GetBuild().down_build("10.121.138.124","root","root")
    #GetBuild().delete_target_node_packege("10.121.137.162","root","root")
    #GetBuild().copy_packege_distance_node("10.121.138.124","root","root","10.121.137.162","root","root")
    path,build_name = GetBuild().get_build_path()
    filepath = "/home/packege/" + build_name
    install_para_config = {"exe_host_info":{'nodeip':"10.121.137.166","username":"root","password":"root"},
                           "node1_info":{'nodeip':"10.121.137.162","username":"root","password":"root"},
                            "node2_info": {'nodeip': "10.121.137.163", "username": "root", "password": "root"},
                           "node3_info":{'nodeip':"10.121.137.164","username":"root","password":"root"},
                            "vip":"10.121.137.169","netmask":"23","filepath":filepath,"clusterscope":"10.121.137.0/23"}
    GetBuild().install_process(**install_para_config)



