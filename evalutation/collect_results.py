from re import compile, search
from pickle import dump
import os

def main():
	all_results = {}
	allj = {}
	all_njobs = [1,2,3,4,5,6,7,8,9,10,15,20,25,30,35,40,45,50]
	schemes = ['random', 'pertask', 'batch', 'sparrrow']
	res_re = compile("results_(random|pertask|batch|sparrrow)_(.*).txt")

	for njobs in all_njobs:
		for scheme in schemes:
			# Generate a result file
			n = str(njobs)
			if len(n) == 1:
				n = "0" + n

			mydir = "testing/%s/" % (n)

			# Remove any previous result file in this folder
			for _, dirs, files in os.walk(mydir):
				fnames = files
				break

                        my_fname = None
			for fname in fnames:
				m = res_re.search(fname)   
				if m != None and m.group(1) == scheme:
					my_fname = fname 
					break
			if my_fname != None:
				print "Found and deleting a previous result for %s %d" % (scheme, njobs)
                                os.remove(mydir + my_fname)


			print "Running result on " + mydir + scheme + "*"
			os.system("./results.sh " + mydir + scheme + "*")

			for _, dirs, files in os.walk(mydir):
				fnames = files
				break

			for fname in fnames:
				m = res_re.search(fname)   
				if m != None and m.group(1) == scheme:
					my_fname = fname 
					break

                        print my_fname
                        final = False
			with open(mydir + my_fname, "r") as f:
				for line in f:
					tkns = line.split()

					if len(line) > 1 and tkns[0] == 'FINAL':
						final = True

					if not final:
						continue
					elif tkns[0] == 'average':
						avg = int(tkns[6])
					elif tkns[0] == 'std_err_bar':
						sem = float(tkns[2])
					elif tkns[0] == 'total' and tkns[3] == 'jobs':
						totalj = int(tkns[5])

			all_results[(scheme, njobs)] = (avg, sem)
			allj[(scheme, njobs)] = totalj

	with open("testing_results.pickle", "wb") as f:
		dump((all_results, allj), f)

if __name__ == '__main__':
    main()
