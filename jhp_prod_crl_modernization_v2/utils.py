
def print_dict_list_table(data):
    from tabulate import tabulate
    if not data:
        print("No data provided.")
        return

    # tabulate automatically uses the dict keys as headers
    print(tabulate(data, headers="keys", tablefmt="grid"))
