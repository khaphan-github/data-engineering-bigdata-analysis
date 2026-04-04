# Simple Ansible Deploy (20 Machines)

Minimal setup to deploy the current cluster to 20 machines.

## 1) Update hosts

Edit:
- `inventories/prod/hosts.yml`

Replace placeholder IPs (`10.0.0.x`) with real machine IPs.

## 2) Update SSH user/key

Edit:
- `inventories/prod/group_vars/all.yml`

Set:
- `ansible_user`
- `ansible_ssh_private_key_file`

## 3) Run deploy

```bash
cd ansible
ansible all -m ping
ansible-playbook playbooks/simple_deploy.yml
```

## Files you need now

- `ansible.cfg`
- `inventories/prod/hosts.yml`
- `inventories/prod/group_vars/all.yml`
- `playbooks/simple_deploy.yml`
