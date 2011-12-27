/* Header file for VMUFAT filesystem */
#ifndef _VMUFAT_H_
#define _VMUFAT_H_

#define VMUFAT_NAMELEN 12

/* GNU utils won't list files with inode num 0 */
#define VMUFAT_ZEROBLOCK 32768
#define VMU_BLK_SZ 512

#define FAT_FILE_END	0xFFFA
#define FAT_UNALLOCATED	0xFFFC
#define FAT_ERROR	0xFFFF

#define VMUFAT_MIN_BLK	0x80
#define VMUFAT_MAX_BLK	0x1000

#define DIR_REC_LEN	0x20
#define DIR_ENT_PER_BLK	0x10

#define VMU_GAME 0xCC
#define VMU_DATA 0x33

static struct kmem_cache *vmufat_inode_cachep;
static struct kmem_cache *vmufat_blist_cachep;
static const struct inode_operations vmufat_inode_operations;
static const struct file_operations vmufat_file_operations;
static const struct address_space_operations vmufat_address_space_operations;
static const struct file_operations vmufat_file_dir_operations;
static const struct super_operations vmufat_super_operations;

static struct inode *vmufat_get_inode(struct super_block *sb, long ino);
static int vmufat_list_blocks(struct inode *in);

/* Linear day numbers of the respective 1sts in non-leap years. */
static int day_n[] =
    {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};

enum vmufat_date {
	VMUFAT_DIR_CENT	=	0x10,
	VMUFAT_DIR_YEAR,
	VMUFAT_DIR_MONTH,
	VMUFAT_DIR_DAY,
	VMUFAT_DIR_HOUR,
	VMUFAT_DIR_MIN,
	VMUFAT_DIR_SEC,
	VMUFAT_DIR_DOW
};

struct memcard {
	unsigned int sb_bnum;
	unsigned int fat_bnum;
	unsigned int fat_len;
	unsigned int dir_bnum;
	unsigned int dir_len;
	unsigned int numblocks;
};

struct vmufat_block_list {
	struct list_head b_list;
	int bno;
};

struct vmufat_inode {
	struct vmufat_block_list blocks;
	int nblcks;
	struct inode vfs_inode;
};

static struct vmufat_inode *VMUFAT_I(struct inode *in)
{
	return container_of(in, struct vmufat_inode, vfs_inode);
}

struct vmufat_file_info {
	u8 ftype;
	u8 copy_pro;
	u16 fblk;
	char fname[VMUFAT_NAMELEN];
};

static inline int vmufat_index(int fno)
{
	return (fno % DIR_ENT_PER_BLK) * DIR_REC_LEN;
}

static inline int vmufat_index_16(int fno)
{
	return (fno % DIR_ENT_PER_BLK) * DIR_REC_LEN / 2;
}
#endif
