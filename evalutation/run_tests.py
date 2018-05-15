from time import sleep
import os
import shutil

def main():
    all_njobs = [1,2,3,4,5,6,7,8,9,10,15,20,25,30,35,40,45,50]
    for _ in range(8):
        for njobs in all_njobs:
            for scheme in ['random', 'pertask', 'batch', 'sparrrow']:
                os.system('./start_sparrow.sh %d %s' % (njobs, scheme))
                print "starting sleep"
                sleep(5)

                while len(os.listdir("logs")) == 0:
                    print "nothing to look at, sleeping a little longer"
                    sleep(5)

                n = str(njobs)
                if len(n) == 1:
                    n = "0" + n
                
                for l in os.listdir("logs"):
                    print "Moving " + l + " to " + "testing/" + n
                    shutil.move("logs/" + l, "testing/" + n)


                os.system("scancel -u \"jlouthan\"")
                sleep(1)

if __name__ == '__main__':
    main()
