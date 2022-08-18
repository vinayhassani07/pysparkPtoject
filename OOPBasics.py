class Person:
    def __init__(self,age):
        #print("new object is form ")
        self.age= age
        print(self.age)
    def sleep(self):
        print("Person is sleeping")
    def get_age(self):
        return self.age



#robert = Person()

tom =Person(20)