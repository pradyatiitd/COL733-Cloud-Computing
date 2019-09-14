from physicalDisk import *
from copy import deepcopy

class DiskManager:
    vd = None
    size = 0
    diskMap = {}
    unoccupiedfragments = []
    usedBlocks = 0
    tot_block = 0
    def __init__(self, sizeA, sizeB):
        self.vd = VirutalDisk(sizeA, sizeB)
        self.size = sizeA + sizeB
        self.unoccupiedfragments = [Fragment(0, self.size)]

    def mergeFragments(self, fraglis):
        f_new = []
        curr_f = fraglis[0]
        for i in range(1, len(fraglis)):
            f = fraglis[i]
            if(f.block == curr_f.block + curr_f.num):
                curr_f.num += f.num
            else:
                f_new.append(curr_f)
                curr_f = f
        f_new.append(curr_f)
        return f_new

    def read(self, block):
        return self.vd.read(block)

    def write(self, block, data):
        self.usedBlocks += 1
        self.vd.write(block, data)

    def createDisk(self, id, num_blocks):
        if(self.size - self.usedBlocks < num_blocks):
            raise Exception("Error: Not enough space")
        elif(id in self.diskMap):
            raise Exception('Error: Disk id is already present')
        else:
            self.createFragment(id, num_blocks)

    def createFragment(self, id, num_blocks):
        if(id not in self.diskMap):
            self.diskMap[id] = Disk(id, num_blocks)
        disk = self.diskMap[id]

        l = [(n, i) for n, i in enumerate(self.unoccupiedfragments) if i.num >= num_blocks]

        if(len(l) == 0):
            p = Fragment(self.unoccupiedfragments[-1].block, self.unoccupiedfragments[-1].num)
            (disk.patches).append(p)
            self.unoccupiedfragments.pop()
            self.usedBlocks += p.num
            self.createFragment(id, num_blocks - p.num)
        else:
            index = l[0][0]
            fragblocknum = l[0][1].block
            fragnum = l[0][1].num
            (disk.patches).append(Fragment(fragblocknum, num_blocks))
            if(fragnum == num_blocks):
                self.unoccupiedfragments.pop(index)
            else:
                currval = fragnum - num_blocks
                while((index > 0) and self.unoccupiedfragments[index-1].num > currval):
                    self.unoccupiedfragments[index].block = self.unoccupiedfragments[index - 1].block
                    self.unoccupiedfragments[index].num = self.unoccupiedfragments[index - 1].num
                    index -= 1
                self.unoccupiedfragments[index].block = fragblocknum + num_blocks
                self.unoccupiedfragments[index].num = currval
            self.usedBlocks += num_blocks

    def getVirtualDiskNo(self, diskpatches, block_no):
        total_blocks = 0
        i = 0
        while(diskpatches[i].num + total_blocks < block_no + 1):
            total_blocks += diskpatches[i].num
            i += 1
        return diskpatches[i].block + block_no - total_blocks

    def readDiskBlock(self, id, block_no):
        if(id not in self.diskMap):
            raise Exception('Invalid Disk Id')
        disk = self.diskMap[id]
        if(disk.numBlocks < block_no + 1):
            raise Exception('Invalid Block Number')
        print("Reading from disk" + str(id))
        vdnum = self.getVirtualDiskNo(disk.patches, block_no)
        return self.vd.read(vdnum)

    def writeDiskBlock(self, id, block_no, data):
        if(id not in self.diskMap):
            raise Exception('Invalid Disk Id')
        disk = self.diskMap[id]
        if(disk.numBlocks < block_no + 1):
            raise Exception('Invalid Block Number')
        print("Writing to disk" + str(id))
        vdnum = self.getVirtualDiskNo(disk.patches, block_no)
        self.vd.write(vdnum, data)
        print("Written data to disk")

    def deleteDisk(self, id):
        if(id not in self.diskMap):
            raise Exception('Invalid Disk Id')
        disk = self.diskMap[id]
        unoccupied = self.unoccupiedfragments + disk.patches
        unocc_sorted_ind = sorted(unoccupied, key = lambda x : x.block)
        unocc_new = self.mergeFragments(unocc_sorted_ind)
        self.unoccupiedfragments = sorted(unocc_new, key = lambda x : x.num)
        self.usedBlocks -= disk.numBlocks
        self.diskMap.pop(id)
        print('Deleted the disk with id ' + str(id))

if __name__ == '__main__':
    dm = DiskManager(300, 200)
    dm.createDisk(1, 100)
    dm.createDisk(2, 200)
    dm.writeDiskBlock(1, 5, "Five")
    dm.writeDiskBlock(2, 10, "Ten")
    # diskPhysical.printDisks()
    print( "Reading disk 2 : ", dm.readDiskBlock(2, 10))
    print( "Reading disk 1 : ", dm.readDiskBlock(1, 5))
    print("Writing to disk 1 : ", dm.writeDiskBlock(1, 5, "Six"))
    print( "Reading disk 1 : ", dm.readDiskBlock(1,5))
    print("Deleting disk 2 : ", dm.deleteDisk(2))
