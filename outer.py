import traceback_new as tn
import sys
import traceback as tb


def main():
    try:
        tn.test()
    except:
      #  print(tb.print_exc())
       # sys.exit(1)
        print(sys.exc_info())


if __name__ == '__main__':
    main()