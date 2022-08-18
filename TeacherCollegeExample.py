class Teacher:
    def __init__(self, name, age, salary):
        self.name = name
        self.age = age
        self.salary = salary

    def get_salary(self):
        return self.salary


class College:

    def __init__(self, name, max_teachers):
        self.name = name
        self.max_teachers = max_teachers
        self.teachers = []

    def add_teachers(self, teacher):
        if len(self.teachers) < self.max_teachers:
            self.teachers.append(teacher)
            return True
        else:
            return False

    def get_avg_sal(self):
        total_sal =0
        for t in self.teachers:
            total_sal = total_sal + t.get_salary()
        avg_sal= total_sal / len(self.teachers)
        return avg_sal


t1 = Teacher('Janvi', 25, 12000)
t2 = Teacher('Riya', 26, 13000)

c1=College("Oxford", 2)
c1.add_teachers(t1)
c1.add_teachers(t2)

print(len(c1.teachers))
print("Average sal is " + str(c1.get_avg_sal()))
