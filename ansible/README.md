# ansible script to operate an elasticell culster in pysical or vm enviroment
ansible-playbook elasticell-clear.yaml -i hosts
ansible-playbook elasticell-config.yaml -i hosts
ansible-playbook elasticell-start.yaml -i hosts
