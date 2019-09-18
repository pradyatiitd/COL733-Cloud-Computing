import random
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
        disk.commandList.append(("readDiskBlock", block_no))
        vdnum = self.getVirtualDiskNo(disk.patches, block_no)
        return self.vd.read(vdnum)

    def writeDiskBlock(self, id, block_no, data):
        if(id not in self.diskMap):
            raise Exception('Invalid Disk Id')
        disk = self.diskMap[id]
        if(disk.numBlocks < block_no + 1):
            raise Exception('Invalid Block Number')
        print("Writing to disk" + str(id))
        disk.commandList.append(("writeDiskBlock", block_no, data))
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

    def readDiskBlockReplicas(self, id, block):
        if(id not in self.diskMap):
            raise Exception('Invalid Disk Id')
        disk = self.diskMap[id]
        disk.commandList.append(("readDiskBlock", block))
        if(disk.numBlocks < block + 1):
            raise Exception('Invalid Block Number')
        if(random.randint(1, 200) < 20):
            print("Bad Block Encountered!")
            if(len(self.unoccupiedfragments) == 0):
                raise Exception("Replica error")
            else:
                #Creating a new block to replace a bad block
                newReplicaBlockn = self.unoccupiedfragments[0].block
                if(self.unoccupiedfragments[0].num == 1):
                    self.unoccupiedfragments.pop(0)
                else:
                    self.unoccupiedfragments[0].num -= 1
                    self.unoccupiedfragments[0].block += 1
                self.usedBlocks += 1

                # Getting the block replica details
                new_patches = []
                virt_orig = self.getVirtualDiskNo(disk.patches, block)
                virt_replica = self.vd.getBlockReplica(virt_orig)
                virt_new_replica = newReplicaBlockn

                # Set replacas to each other since the original block is
                # erreneous
                self.vd.setBlockReplica(virt_replica, virt_new_replica)
                self.vd.setBlockReplica(virt_new_replica, virt_replica)

                # Reading data from Replica and Writing to new replica
                ans = self.vd.read(virt_replica)
                self.vd.write(virt_new_replica, ans)

                neworig = Fragment(virt_replica, 1)
                newReplica = Fragment(virt_new_replica, 1)

                for p in disk.patches:
                    if((virt_orig < p.block or virt_replica >= (p.block + p.num)) and (virt_orig < p.block or virt_orig >= (p.block + p.num))):
                        new_patches.append(p)

                    elif((virt_orig < p.block or virt_orig > (p.block + p.num)) and p.block <= virt_replica and virt_replica < (p.block + p.num)):
                        if(virt_replica > p.block):
                            left_p = Fragment(p.block, virt_replica - p.block)
                            new_patches.append(left_p)
                        new_patches.append(newReplica)
                        if(virt_replica < (p.block + p.num - 1)):
                            right_p = Fragment(virt_replica + 1, p.num  - (virt_replica - p.block + 1))
                            new_patches.appned(right_p)

                    elif((virt_replica > (p.block + p.num) or virt_replica < p.block) and p.block <= virt_orig and virt_orig < (p.block + p.num)):
                        if(virt_orig > p.block):
                            left_p = Fragment(p.block, virt_orig - p.block)
                            new_patches.append(left_p)
                        new_patches.append(neworig)
                        if(virt_orig < (p.block + p.num + 1)):
                            right_p = Fragment(virt_orig + 1, p.num - (virt_orig - p.block + 1))
                            new_patches.append(right_p)

                    else:
                        if(virt_orig > p.block):
                            left_p = Fragment(p.block, virt_orig - p.block)
                            new_patches.append(left_p)
                        new_patches.append(neworig)
                        if(virt_orig < virt_replica - 1):
                            mid_p = Fragment(virt_orig + 1, virt_replica  - virt_orig - 1)
                            new_patches.append(mid_p)
                        new_patches.append(newReplica)
                        if(virt_replica < (p.block + p.num - 1)):
                            right_p = Fragment(virt_replica + 1, p.num - (virt_replica - p.block + 1))
                            new_patches.append(right_p)

                    disk.patches = self.mergeFragments(new_patches)

        else:
            print("Reading from the disk" + str(id))
            vdnum = self.getVirtualDiskNo(disk.patches, block)
            ans = self.vd.read(vdnum)
        return ans

    def writeDiskBlockReplicas(self, id, block, data):
        if(id not in self.diskMap):
            raise Exception('Invalid Disk Id')
        disk = self.diskMap[id]
        if(disk.numBlocks < block + 1):
            raise Exception('Invalid Block Number')
        # Writing data in primary block
        disk.commandList.append(("writeDiskBlock", block, data))
        vdnum = self.getVirtualDiskNo(disk.patches, block)
        self.vd.write(vdnum, data)

        # Creating a replica
        delta = disk.numBlocks // 2
        replica_disk = (block + delta) if block < delta else block - delta
        virtual_replica_block = self.getVirtualDiskNo(disk.patches, replica_disk)
        curr_replica = self.vd.getBlockReplica(vdnum)
        if(curr_replica == -1 or curr_replica != virtual_replica_block):
            self.vd.setBlockReplica(vdnum, virtual_replica_block)
            self.vd.setBlockReplica(virtual_replica_block, vdnum)
        varia = self.vd.getBlockReplica(vdnum)
        # writing data to replica
        self.vd.write(varia, data)
        print("Written to disk")

    def checkPoint(self,id):
        if(id not in self.diskMap):
            raise Exception('Invalid Disk Id')
        disk = self.diskMap[id]
        disk.checkPointMap.append(len(disk.commandList))
        return len(disk.checkPointMap)-1

    def rollback(self,disk_id,checkpoint_id):
        if(disk_id not in self.diskMap):
            raise('Invalid Disk Id')
        disk = self.diskMap[disk_id]
        checkpoints = disk.checkPointMap[0:checkpoint_id]
        commands = disk.commandList[0:disk.checkPointMap[checkpoint_id]]

        self.deleteDisk(disk_id)

        for command in commands:
            if command[0] == "createDisk":
                self.createDisk(command[1],command[2])
                disk = self.diskMap[disk_id]
            elif command[0] == "readDiskBlock":
                x = self.readDiskBlock(disk_id,command[1])
            else:
                self.writeDiskBlock(disk_id,command[1],command[2])

        disk.checkPointMap = checkpoints
        disk.commandList = commands


if __name__ == '__main__':
    dm = DiskManager(300, 200)
    dm.createDisk(1, 100)
    dm.createDisk(2, 200)
    dm.writeDiskBlockReplicas(1, 5, "Five")
    dm.writeDiskBlockReplicas(2, 10, "Ten")
    dm.writeDiskBlockReplicas(1, 1, "Five")
    dm.writeDiskBlockReplicas(2, 2, "Ten")
    # diskPhysical.printDisks()
    print( "Checkpoint disk 1 : ", dm.checkPoint(1))
    print( "Reading disk 2 : ", dm.readDiskBlockReplicas(2, 10))
    print( "Reading disk 1 : ", dm.readDiskBlockReplicas(1, 5))
    # print( "Reading disk 1 : ", dm.readDiskBlock(1,5))
    print("Writing to disk 1 : ", dm.writeDiskBlockReplicas(1, 5, "Six"))
    print( "Rolling back disk 1 : ", dm.rollback(1,0))
    print( "Reading disk 1 : ", dm.readDiskBlockReplicas(1,5))
    print("Deleting disk 2 : ", dm.deleteDisk(2))
