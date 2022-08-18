class Pet:
    no_of_pet=0

    def __init__(self, name, age):
        self.name = name
        self.age = age
        Pet.no_of_pet += 1

#print(Pet.no_of_pet)


p1=Pet('dog', 1)
p2=Pet('cat', 2)
p3=Pet('cow', 3)
print(Pet.no_of_pet)
