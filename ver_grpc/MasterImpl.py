workers_list = []


# Functions
def add_node(node):
    workers_list.append(node)


def remove_node(node):
    workers_list.remove(node)


def get_workers():
    return workers_list
