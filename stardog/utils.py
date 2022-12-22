def strtobool(s):
    truthy_values = ["y", "yes", "t", "true", "True", "on", 1]
    falsy_values = ["n", "no", "f", "false", "False", "off", 0]
    if s in truthy_values:
        return True
    if s in falsy_values:
        return False
    raise ValueError
