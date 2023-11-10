def checkDefined(name, value):
    if value is None:
        raise ValueError("'%s' is not defined" % name)