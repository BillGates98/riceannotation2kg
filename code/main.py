
from migrator import Migrator
from parser import Parser 

_ = Parser(input_path='../json/', extensions=['.json']).run()
print('Oryza .json ended 100 %')

