import os
import re
import csv
from datetime import datetime
from tqdm import tqdm
date_time = datetime.now()

########## SCHEMA OF TABLES #############

### product.csv
		# ID
		# ASIN
		# TITLE
		# GROUP
		# SALESRANK
		# NUMBER OF SIMILAR 
		# NUMBER OF CATEGORIES 
		# NUMBER OF REVIEWS 
		# NUMBER OF DOWNLOADS 
		# AVERAGE RATING

### similar.csv
		# ID 
		# ASIN 
		# SIMILAR ASIN 
		# (one entry for each similar object)

### categories.csv 
		# ID 
		# ASIN 
		# STRING 
		# (one entry for each cateogry)

### reviews.csv
		# ID 
		# ASIN
		# DATE 
		# CUSTOMER 
		# RATING 
		# VOTES 
		# HELPFUL 
		# (one entry for each review )
############################################


# Getting details
input_file = 'amazon-meta.txt'
output_dir = str(date_time) +'/'
if not os.path.exists(output_dir):
	os.makedirs(output_dir)

#reading file
print('Loading input file : ' + str(input_file))
with open(input_file,'r') as f:
	content = f.read()
	patern = re.compile("\n\n")
	blocks = patern.split(content) # Dividing text file into blocks of data separated by white line
print("Done!")

print("Reading file and generating CSVs...")
#reading block
b = 0
for block in tqdm(blocks): #reading each block and storing information for 4 csv's
	lines = block.split('\n')
	flag1 = (lines[0].split(':')[0])=='Id' #checking if its product data
	flag2 = False
	if flag1:
		flag2 = not((lines[2]) =='  discontinued product')

	product_info = []
	similar_info = []
	category_info = []
	review_info = []

	if flag2:
		i = 0
		while i < len(lines):
			#id
			if lines[i].split(':')[0] == 'Id':
				product_info.append((lines[i].split(':')[1]).strip())
				i+=1
				continue
			#asin
			if lines[i].split(':')[0] == 'ASIN':
				product_info.append((lines[i].split(':')[1]).strip())
				i+=1
				continue
			#title
			if lines[i].split(':')[0] == '  title':
				title = lines[i][8:].strip('"').strip()
				product_info.append(title)
				i+=1
				continue
			#group 
			if lines[i].split(':')[0] == '  group':
				product_info.append(lines[i].split(':')[1].strip())
				i+=1
				continue
			#salesrank
			if lines[i].split(':')[0] == '  salesrank':
				product_info.append((lines[i].split(':')[1]).strip())
				i+=1
				continue
			#similar 
			if lines[i].split(':')[0] == '  similar':
				parts = lines[i].split(':')[1].split('  ')
				product_info.append((parts[0]).strip())
				
				for j in range(1,int(parts[0])+1):
					s = []
					s.append(product_info[0])
					s.append(product_info[1])
					s.append(parts[j].strip())
					similar_info.append(s)
				i +=1
				continue
			# categories
			if lines[i].split(':')[0] =='  categories':
				no_of_catg = lines[i].split(':')[1].strip()
				product_info.append(no_of_catg)
				filtered_sub_catg = []

				for j in range(1,int(no_of_catg)+1):
					
					catg = lines[i+j].split('|')
					[filtered_sub_catg.append(c) for c in catg if c not in filtered_sub_catg]

				if len(filtered_sub_catg) > 0:
					for sub_catg in filtered_sub_catg:
						if sub_catg == '   ':
							continue
						c = []
						c.append(product_info[0])
						c.append(product_info[1])
						c.append(sub_catg)
						category_info.append(c)




				i = i+int(no_of_catg)+1
				continue
			#reviews
			if lines[i].split(':')[0] =='  reviews':
				flag3 = True
				no_of_rev = 0
				downloads = (lines[i].split(':')[3]).split('  ')[0].strip()
				avg_rating = (lines[i]).split(':')[4].strip()
				while flag3:
					try:
						next_line = lines[i+1]
					except:
						flag3 = False
						continue 

					no_of_rev = no_of_rev+1
					r = []
					r.append(product_info[0])
					r.append(product_info[1])

					r.append((lines[i+1].split(':')[0].split('  ')[-2]).strip())
					r.append((lines[i+1].split(':')[1].split('  ')[-2]).strip())
					r.append((lines[i+1].split(':')[2].split('  ')[-2]).strip())
					r.append((lines[i+1].split(':')[3].split('  ')[-2]).strip())
					r.append((lines[i+1].split(':')[4].split('  ')[-1]).strip())
					review_info.append(r)
					i = i+1

				i = i+1
				product_info.append(no_of_rev)
				product_info.append(downloads)
				product_info.append(avg_rating)
				continue
				
			else:
				i += 1

		with open(output_dir + 'product.csv','a') as prod :
			writer = csv.writer(prod,delimiter = '^')
			writer.writerow(product_info)
		with open(output_dir + 'similar.csv','a') as sim :
			writer = csv.writer(sim,delimiter = '^')
			writer.writerows(similar_info)
		with open(output_dir + 'categories.csv','a') as catg :
			writer = csv.writer(catg,delimiter = '^')
			writer.writerows(category_info)
		with open(output_dir + 'reviews.csv','a') as rev :
			writer = csv.writer(rev,delimiter = '^')
			writer.writerows(review_info)

	

	