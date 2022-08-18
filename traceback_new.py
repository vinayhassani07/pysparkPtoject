import traceback as tb
def test():
    sal=-1
    try:
        if sal<0:
            raise  Exception('Salary can not be less than 0')
    except Exception as arg:
        print("Exception !!!", arg)
        raise
