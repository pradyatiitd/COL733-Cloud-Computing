import random
class Block:
    blockData = ''
    replica = -1

class PhysicalDisk:
    size = 0
    disk = []
    usedBlocks = 0
    def __init__(self, size):
        self.size = size
        self.disk = [Block() for i in range(self.size)]

    def getsize(self):
        return self.size

    def getusedBlocks(self):
        return self.usedBlocks

    def writeData(self, block, data):
        self.disk[block].blockData = data
        self.usedBlocks += 1

    def readData(self, block):
        return self.disk[block].blockData

    def printDisK(self):
        for i in range(self.size):
            print("Relative ID: " + str(i) + " Data: " + str(disk[i].data))

    def getReplica(self, block):
        return self.disk[block].replica

    def setReplica(self, block, replica_block_num_virt):
        self.disk[block].replica = replica_block_num_virt

class VirutalDisk:
    disks = []
    sizes = []
    usedBlocks = 0
    map = {}
    def __init__(self, sizeA, sizeB):
        self.sizes = [sizeA, sizeB]
        self.disks = [PhysicalDisk(sizeA), PhysicalDisk(sizeB)]
        def vtoPhyMap(disks):
            tot = 0
            map = {}
            for i in range(len(disks)):
                n = disks[i].getsize()
                for j in range(n):
                    map[tot + j] = (i, j)
                tot += n
            return map
        self.map = vtoPhyMap(self.disks)

    def write(self, block, data):
        (n, loc) = self.map[block]
        self.disks[n].writeData(loc, data)
        self.usedBlocks += 1

    def read(self, block):
        (n, loc) = self.map[block]
        return self.disks[n].readData(loc)

    def getBlockReplica(self, block):
        (n, loc) = self.map[block]
        return self.disks[n].getReplica(loc)

    def setBlockReplica(self, block, replica_block_num_virt):
        (n, loc) = self.map[block]
        self.disks[n].setReplica(loc, replica_block_num_virt)

class Fragment:
    block = 0
    num = 0
    def __init__(self, block, n):
        self.block = block
        self.num = n

class Disk:
	id = 0
	numBlocks = 0
	commandList = []
	checkPointMap = []
	patches = []

	def __init__(self, idname, n):
		self.id = idname
		self.numBlocks = n
		self.patches = []
		self.commandList = [("createDisk", idname, n)]
		self.checkPointMap = []


# if __name__ == '__main__':
#     vd = VirutalDisk(200, 300)
#     # pd = PhysicalDisk(200)
#     # print(pd.getsize())
#     l = []
#     for i in range(10):
#         b = random.randint(0, 500)
#         l.append(b)
#         vd.write(b, str(b))
#     for i in range(len(l)):
#         if(vd.read(l[i]) != ''):
#             print(vd.read(l[i]))
