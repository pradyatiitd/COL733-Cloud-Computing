import random
from random import randint
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
        l = []
        for n in range(len(self.unoccupiedfragments)):
            i = self.unoccupiedfragments[n]
            if(i.num >= num_blocks):
                l.append((n, i));
        disk = self.diskMap[id]
        if(len(l) == 0):
            bl, nu = self.unoccupiedfragments[-1].block, self.unoccupiedfragments[-1].num
            disk.fragments.append(Fragment(bl, nu))
            self.unoccupiedfragments.pop()
            self.usedBlocks += nu
            self.createFragment(id, num_blocks - nu)
        else:
            index = l[0][0]
            disk.fragments.append(Fragment(l[0][1].block, num_blocks))
            if(l[0][1].num == num_blocks):
                self.unoccupiedfragments.pop(index)
            else:
                currval = l[0][1].num - num_blocks
                while((index > 0) and self.unoccupiedfragments[index-1].num > currval):
                    index -= 1
                    nuuu, boo = self.unoccupiedfragments[index - 1].num, self.unoccupiedfragments[index - 1].block
                    self.unoccupiedfragments[index].num = nuuu
                    self.unoccupiedfragments[index].block = boo
                self.unoccupiedfragments[index].block = l[0][1].block + num_blocks
                self.unoccupiedfragments[index].num = currval
            self.usedBlocks += num_blocks

    def getVirtualDiskNo(self, diskpatches, block_no):
        i = 0
        total_blocks = 0
        while(diskpatches[i].num + total_blocks <= block_no):
            i += 1
            total_blocks += diskpatches[i].num
        return diskpatches[i].block + block_no - total_blocks

    def readDiskBlock(self, id, block_no):
        if(id not in self.diskMap):
            raise Exception('Invalid Disk Id')
        disk = self.diskMap[id]
        if(disk.numberOfBlocks < block_no + 1):
            raise Exception('Invalid Block Number')
        print("Reading from disk" + str(id))
        disk.commands.append(("readDiskBlock", block_no))
        vdnum = self.getVirtualDiskNo(disk.fragments, block_no)
        return self.vd.read(vdnum)

    def writeDiskBlock(self, id, block_no, data):
        if(id not in self.diskMap):
            raise Exception('Invalid Disk Id')
        disk = self.diskMap[id]
        if(disk.numberOfBlocks < block_no + 1):
            raise Exception('Invalid Block Number')
        print("Writing to disk" + str(id))
        disk.commands.append(("writeDiskBlock", block_no, data))
        vdnum = self.getVirtualDiskNo(disk.fragments, block_no)
        self.vd.write(vdnum, data)
        print("Written data to disk")

    def deleteDisk(self, id):
        if(id not in self.diskMap):
            print('Invalid Disk Id')
            return;
        disk = self.diskMap[id]
        self.usedBlocks -= disk.numberOfBlocks
        unoccupied = self.unoccupiedfragments + disk.fragments
        def sortArray(l, b = 0):
            if(b == 0):
                return sorted(l, key = lambda x : x.block)
            else:
                return sorted(l, key = lambda x : x.num)
        unocc_sorted_ind = sortArray(unoccupied)
        unocc_new = self.mergeFragments(unocc_sorted_ind)
        self.unoccupiedfragments = sortArray(unocc_new, 1)
        self.diskMap.pop(id)
        print('Deleted the disk with id ' + str(id))

    def readDiskBlockReplicas(self, id, block):
        if(id not in self.diskMap):
            raise Exception('Invalid Disk Id')
        disk = self.diskMap[id]
        disk.commands.append(("readDiskBlock", block))
        if(disk.numberOfBlocks < block + 1):
            print("Cannot make replica")
            return;
        random.seed(0)
        if(randint(1, 200) > 180):
            print("Bad Block Encountered!")
            if(len(self.unoccupiedfragments) == 0):
                print("Replica error")
                return;
            else:
                #Creating a new block to replace a bad block
                newReplicaBlockn = self.unoccupiedfragments[0].block
                if(self.unoccupiedfragments[0].num != 1):
                    self.unoccupiedfragments[0].block += 1
                    self.unoccupiedfragments[0].num -= 1
                else:
                    self.unoccupiedfragments.pop(0)
                self.usedBlocks += 1

                # Getting the block replica details
                new_patches = []
                virt_orig = self.getVirtualDiskNo(disk.fragments, block)
                virt_replica = self.vd.getBlockReplica(virt_orig)
                virt_new_replica = newReplicaBlockn

                # Set replacas to each other since the original block is
                # erreneous
                self.vd.setBlockReplica(virt_replica, virt_new_replica)
                self.vd.setBlockReplica(virt_new_replica, virt_replica)

                # Reading data from Replica and Writing to new replica
                ans = self.vd.read(virt_replica)
                self.vd.write(virt_new_replica, ans)

                for p in disk.fragments:
                    if((virt_orig < p.block or virt_orig >= (p.block + p.num - 1)) and (p.block - 1) < virt_replica and (virt_replica - 1) <= (p.block + p.num)):
                        if(virt_replica >= p.block - 1):
                            new_patches.append(Fragment(p.block, virt_replica - p.block))
                        new_patches.append(Fragment(virt_new_replica, 1))
                        if((virt_replica + 1) < (p.block + p.num)):
                            new_patches.appned(Fragment(virt_replica + 1, p.num  - (virt_replica - p.block + 1)))

                    elif(((virt_orig - 1) <= p.block or virt_orig >= (p.block + p.num)) and (virt_orig - 1 <= p.block or virt_replica >= (p.block + p.num))):
                        new_patches.append(p)

                    elif(p.block <= virt_orig and virt_orig < (p.block + p.num) and (virt_replica > (p.block + p.num) or virt_replica < p.block)):
                        if(virt_orig > p.block):
                            new_patches.append(Fragment(p.block, virt_orig - p.block))
                        new_patches.append(Fragment(virt_replica, 1))
                        if(virt_orig <= (p.block + p.num)):
                            new_patches.append(Fragment(virt_orig + 1, p.num - (virt_orig - p.block + 1)))

                    else:
                        if(virt_orig >= p.block - 1):
                            new_patches.append(Fragment(p.block, virt_orig - p.block))
                        new_patches.append(Fragment(virt_replica, 1))
                        if(virt_orig <= virt_replica - 2):
                            new_patches.append(Fragment(virt_orig + 1, virt_replica  - virt_orig - 1))
                        new_patches.append(Fragment(virt_new_replica, 1))
                        if(virt_replica + 1 < (p.block + p.num)):
                            new_patches.append(Fragment(virt_replica + 1, p.num - (virt_replica - p.block + 1)))

                    disk.fragments = self.mergeFragments(new_patches)

        else:
            print("Reading from the disk" + str(id))
            vdnum = self.getVirtualDiskNo(disk.fragments, block)
            ans = self.vd.read(vdnum)
        return ans

    def writeDiskBlockReplicas(self, id, block, data):
        if(id not in self.diskMap):
            raise Exception('Invalid Disk Id')
        disk = self.diskMap[id]
        if(disk.numberOfBlocks < block + 1):
            raise Exception('Invalid Block Number')
        # Writing data in primary block
        disk.commands.append(("writeDiskBlock", block, data))
        vdnum = self.getVirtualDiskNo(disk.fragments, block)
        self.vd.write(vdnum, data)

        # Creating a replica
        delta = disk.numberOfBlocks // 2
        replica_disk = (block + delta) if block < delta else block - delta
        virtual_replica_block = self.getVirtualDiskNo(disk.fragments, replica_disk)
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
        disk.checkPoints.append(len(disk.commands))
        return len(disk.checkPoints)-1

    def rollback(self,disk_id,checkpoint_id):
        if(disk_id not in self.diskMap):
            raise('Invalid Disk Id')
        disk = self.diskMap[disk_id]
        checkpoints = disk.checkPoints[0 :checkpoint_id]
        commands = disk.commands[0 : disk.checkPoints[checkpoint_id]]

        self.deleteDisk(disk_id)

        for command in commands:
            if(command[0] == "createDisk"):
                self.createDisk(command[1], command[2])
                disk = self.diskMap[disk_id]
            elif(command[0] == "readDiskBlock"):
                x = self.readDiskBlock(disk_id, command[1])
            elif(command[0] == "writeDiskBlock"):
                self.writeDiskBlock(disk_id, command[1], command[2])

        disk.checkPoints = deepcopy(checkpoints)
        disk.commands = deepcopy(commands)


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
