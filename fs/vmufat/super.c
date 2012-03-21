/*
 * VMUFAT file system
 *
 * Copyright (C) 2002 - 2012	Adrian McMenamin
 * Copyright (C) 2002		Paul Mundt
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 */

#include <linux/fs.h>
#include <linux/bcd.h>
#include <linux/rtc.h>
#include <linux/slab.h>
#include <linux/sched.h>
#include <linux/magic.h>
#include <linux/device.h>
#include <linux/module.h>
#include <linux/statfs.h>
#include <linux/buffer_head.h>
#include "vmufat.h"

static struct kmem_cache *vmufat_inode_cachep;
static const struct super_operations vmufat_super_operations;
extern int *day_n;
extern struct kmem_cache *vmufat_blist_cachep;
extern const struct inode_operations vmufat_inode_operations;
extern const struct file_operations vmufat_file_operations;
extern const struct address_space_operations vmufat_address_space_operations;
extern const struct file_operations vmufat_file_dir_operations;

static long vmufat_get_date(struct buffer_head *bh, int offset)
{
	int century, year, month, day, hour, minute, second;
	if (!bh)
		return -EINVAL;

	century = bcd2bin(bh->b_data[offset++]);
	year = bcd2bin(bh->b_data[offset++]);
	month = bcd2bin(bh->b_data[offset++]);
	day = bcd2bin(bh->b_data[offset++]);
	hour = bcd2bin(bh->b_data[offset++]);
	minute = bcd2bin(bh->b_data[offset++]);
	second = bcd2bin(bh->b_data[offset]);

	return mktime(century * 100 + year, month, day, hour, minute,
		second);
}

static struct inode *vmufat_alloc_inode(struct super_block *sb)
{
	struct vmufat_inode *vi = kmem_cache_alloc(vmufat_inode_cachep,
		GFP_KERNEL);
	if (!vi)
		return NULL;

	INIT_LIST_HEAD(&vi->blocks.b_list);
	return &vi->vfs_inode;
}

static void vmufat_destroy_inode(struct inode *in)
{
	struct vmufat_inode *vi;
	struct vmufat_block_list *vb;
	struct list_head *iter, *iter2;
	if (!in)
		return;
	vi = VMUFAT_I(in);
	if (!vi)
		return;

	list_for_each_safe(iter, iter2, &vi->blocks.b_list) {
		vb = list_entry(iter, struct vmufat_block_list, b_list);
		list_del(iter);
		kmem_cache_free(vmufat_blist_cachep, vb);
	}
	kmem_cache_free(vmufat_inode_cachep, vi);
}

struct inode *vmufat_get_inode(struct super_block *sb, long ino)
{
	struct buffer_head *bh = NULL;
	int error = 0, i, j, found = 0;
	int offsetindir;
	struct inode *inode;
	struct memcard *vmudetails;
	long superblock_bno;
	if (!sb || !sb->s_fs_info) {
		error = -EINVAL;
		goto reterror;
	}

	vmudetails = sb->s_fs_info;
	inode = iget_locked(sb, ino);
	if (!inode) {
		error = -EIO;
		goto reterror;
	}
	superblock_bno = vmudetails->sb_bnum;

	if (inode->i_state & I_NEW) {
		inode->i_uid = current_fsuid();
		inode->i_gid = current_fsgid();
		inode->i_mode &= ~S_IFMT;
		if (inode->i_ino == superblock_bno) {
			bh = vmufat_sb_bread(sb, inode->i_ino);
			if (!bh) {
				error = -EIO;
				goto failed;
			}
			inode->i_ctime.tv_sec = inode->i_mtime.tv_sec =
				vmufat_get_date(bh, VMUFAT_SB_DATEOFFSET);

			/* Mark as a directory */
			inode->i_mode = S_IFDIR | S_IRUGO | S_IXUGO;
			inode->i_op = &vmufat_inode_operations;
			inode->i_fop = &vmufat_file_dir_operations;
		} else {
			/* Mark file as regular type */
			inode->i_mode = S_IFREG | S_IRUGO | S_IWUSR;

			/* Scan through the directory to find matching file */
			for (i = vmudetails->dir_bnum;
				i > vmudetails->dir_bnum - vmudetails->dir_len;
				i--) {
				brelse(bh);
				bh = vmufat_sb_bread(sb, i);
				if (!bh) {
					error = -EIO;
					goto failed;
				}
				for (j = 0; j < VMU_DIR_ENTRIES_PER_BLOCK; j++) {
					if (bh->b_data[j * VMU_DIR_RECORD_LEN]
						== 0)
						goto notfound;
					if (le16_to_cpu(((u16 *) bh->b_data)
					[j * VMU_DIR_RECORD_LEN16 +
					VMUFAT_FIRSTBLOCK_OFFSET16]) == ino) {
						found = 1;
						goto found;
					}
				}
			}
notfound:
			error = -ENOENT;
			goto failed;
found:
			/* identified the correct directory entry */
			offsetindir = j * VMU_DIR_RECORD_LEN;
			inode->i_ctime.tv_sec = inode->i_mtime.tv_sec =
				vmufat_get_date(bh,
					offsetindir + VMUFAT_FILE_DATEOFFSET);

			/* Execute if a game, write if not copy protected */
			inode->i_mode &= ~(S_IWUGO | S_IXUGO);
			inode->i_mode |= S_IRUGO;

			/* Mode - is it write protected? */
			if ((((u8 *) bh->b_data)[0x01 + offsetindir] ==
			     0x00) & ~(sb->s_flags & MS_RDONLY))
				inode->i_mode |= S_IWUSR;
			/* Is file executible - ie a game */
			if ((((u8 *) bh->b_data)[offsetindir] ==
			     0xcc) & ~(sb->s_flags & MS_NOEXEC))
				inode->i_mode |= S_IXUSR;

			inode->i_fop = &vmufat_file_operations;

			inode->i_blocks =
			    le16_to_cpu(((u16 *) bh->b_data)
				[offsetindir / 2 + 0x0C]);
			inode->i_size = inode->i_blocks * sb->s_blocksize;

			inode->i_mapping->a_ops =
				&vmufat_address_space_operations;
			inode->i_op = &vmufat_inode_operations;
			inode->i_fop = &vmufat_file_operations;
			error = vmufat_list_blocks(inode);
			if (error)
				goto failed;
		}
		inode->i_atime = CURRENT_TIME;
		unlock_new_inode(inode);
	}
	brelse(bh);
	return inode;

failed:
	iget_failed(inode);
reterror:
	brelse(bh);
	return ERR_PTR(error);
}

static void vmufat_put_super(struct super_block *sb)
{
	if (!sb)
		return;
	sb->s_dev = 0;
	kfree(sb->s_fs_info);
}

static int vmufat_count_freeblocks(struct super_block *sb,
					struct kstatfs *kstatbuf)
{
	int error = 0;
	int free = 0;
	int i;
	struct memcard *vmudetails;

	if (!sb || !sb->s_fs_info || !kstatbuf) {
		error = -EINVAL;
		goto out;
	}
	vmudetails = sb->s_fs_info;

	/* Look through the FAT */
	for (i = 0; i < vmudetails->numblocks; i++) {
		if (vmufat_get_fat(sb, i) == VMUFAT_UNALLOCATED)
			free++;
	}
	kstatbuf->f_bfree = free;
	kstatbuf->f_bavail = free;
	kstatbuf->f_blocks = vmudetails->numblocks;
out:
	return error;
}

static int vmufat_statfs(struct dentry *dentry, struct kstatfs *kstatbuf)
{
	int error;
	struct super_block *sb;

	if (!dentry || !kstatbuf) {
		error = -EINVAL;
		goto out;
	}

	sb = dentry->d_sb;
	if (!sb) {
		error = -EINVAL;
		goto out;
	}
	error = vmufat_count_freeblocks(sb, kstatbuf);
	if (error)
		goto out;
	kstatbuf->f_type = VMUFAT_MAGIC;
	kstatbuf->f_bsize = sb->s_blocksize;
	kstatbuf->f_namelen = VMUFAT_NAMELEN;
out:
	return error;
}

/* Remove inode from memory */
static void vmufat_evict_inode(struct inode *in)
{
	if (!in)
		return;

	truncate_inode_pages(&in->i_data, 0);
	invalidate_inode_buffers(in);
	in->i_size = 0;
	end_writeback(in);
}

/*
 * There are no inodes on the medium - vmufat_write_inode
 * updates the directory entry
 */
static int vmufat_write_inode(struct inode *in, struct writeback_control *wbc)
{
	struct buffer_head *bh = NULL;
	unsigned long inode_num;
	int i, j, found = 0;
	struct super_block *sb;
	struct memcard *vmudetails;
	if (!in || !in->i_sb)
		return -EINVAL;
	sb = in->i_sb;
	vmudetails = sb->s_fs_info;
	if (!vmudetails)
		return -EINVAL;

	/* As most real world devices are flash we
	 * won't update the superblock every time */
	if (in->i_ino == vmudetails->sb_bnum)
		return 0;
	if (in->i_ino == VMUFAT_ZEROBLOCK)
		inode_num = 0;
	else
		inode_num = in->i_ino;

	/* update the directory and inode details */
	/* Now search for the directory entry */
	down_interruptible(&vmudetails->vmu_sem);
	for (i = vmudetails->dir_bnum;
		i > vmudetails->dir_bnum - vmudetails->dir_len; i--) {
		bh = vmufat_sb_bread(sb, i);
		if (!bh) {
			up(&vmudetails->vmu_sem);
			return -EIO;
		}
		for (j = 0; j < VMU_DIR_ENTRIES_PER_BLOCK; j++) {
			if (bh->b_data[j * VMU_DIR_RECORD_LEN] == 0) {
				up(&vmudetails->vmu_sem);
				brelse(bh);
				return -ENOENT;
			}
			if (le16_to_cpu(((u16 *)bh->b_data)
				[j * VMU_DIR_RECORD_LEN16 +
				VMUFAT_FIRSTBLOCK_OFFSET16]) == inode_num) {
				found = 1;
				goto found;
			}
		}
		brelse(bh);
	}
found:
	if (found == 0) {
		up(&vmudetails->vmu_sem);
		return -EIO;
	}

	/* Have the directory entry
	 * so now update it */
	if (inode_num != 0)
		bh->b_data[j * VMU_DIR_RECORD_LEN] = VMU_DATA;	/* data file */
	else
		bh->b_data[j * VMU_DIR_RECORD_LEN] = VMU_GAME;
	if (bh->b_data[j * VMU_DIR_RECORD_LEN + 1] !=  0
	    && bh->b_data[j * VMU_DIR_RECORD_LEN + 1] != (char) 0xff)
		bh->b_data[j * VMU_DIR_RECORD_LEN + 1] = 0;
	((u16 *) bh->b_data)[j * VMU_DIR_RECORD_LEN16 + 1] =
		cpu_to_le16(inode_num);

	/* BCD timestamp it */
	in->i_mtime = CURRENT_TIME;
	vmufat_save_bcd(in, bh->b_data, j * VMU_DIR_RECORD_LEN);

	((u16 *) bh->b_data)[j * VMU_DIR_RECORD_LEN16 + 0x0C] =
		cpu_to_le16(in->i_blocks);
	if (inode_num != 0)
		((u16 *) bh->b_data)[j * VMU_DIR_RECORD_LEN16 + 0x0D] = 0;
	else /* game */
		((u16 *) bh->b_data)[j * VMU_DIR_RECORD_LEN16 + 0x0D] =
			cpu_to_le16(1);
	up(&vmudetails->vmu_sem);
	mark_buffer_dirty(bh);
	brelse(bh);
	return 0;
}

static int check_sb_format(struct buffer_head *bh)
{
	if (!bh)
		return -EINVAL;

	return (((u32 *) bh->b_data)[0] == VMUFAT_MAGIC &&
		((u32 *) bh->b_data)[1] == VMUFAT_MAGIC &&
		((u32 *) bh->b_data)[2] == VMUFAT_MAGIC &&
		((u32 *) bh->b_data)[3] == VMUFAT_MAGIC);
}

static void vmufat_populate_vmudata(struct memcard *vmudata,
		struct buffer_head *bh, int test_sz)
{
	vmudata->sb_bnum = test_sz - 1;
	vmudata->fat_bnum =
		le16_to_cpu(((u16 *) bh->b_data)[VMU_LOCATION_FAT]);
	vmudata->fat_len =
		le16_to_cpu(((u16 *) bh->b_data)[VMU_LOCATION_FATLEN]);
	vmudata->dir_bnum =
		le16_to_cpu(((u16 *) bh->b_data)[VMU_LOCATION_DIR]);
	vmudata->dir_len =
		le16_to_cpu(((u16 *) bh->b_data)[VMU_LOCATION_DIRLEN]);
	/* return the true number of user available blocks - VMUs
 	 * return a neat 200 and ignore 40 blocks of usable space -
 	 * we get round that in a hardware neutral way */
	vmudata->numblocks = vmudata->dir_bnum - vmudata->dir_len + 1;
}

static int vmufat_get_size(struct super_block *sb, struct buffer_head **bh)
{
	int i;

	for (i = VMUFAT_MIN_BLK; i <= VMUFAT_MAX_BLK; i = i * 2) {
		brelse(*bh);
		*bh = vmufat_sb_bread(sb, i - 1);
		if (*bh == NULL) {
			i = -EIO;
			goto out;
		}
		if (check_sb_format(*bh))
			break;
	}
	if (i > VMUFAT_MAX_BLK) {
		brelse(*bh);
		i = -ENOENT;
	}
out:
	return i;
}

static int vmufat_fill_super(struct super_block *sb,
					    void *data, int silent)
{
	struct buffer_head *bh = NULL;
	struct memcard *vmudata;
	int test_sz;
	struct inode *root_i;
	int ret = -EINVAL;
	if (!sb)
		goto out;

	sb_set_blocksize(sb, VMU_BLK_SZ);
	sb->s_blocksize_bits = ilog2(VMU_BLK_SZ);
	sb->s_magic = VMUFAT_MAGIC;
	sb->s_op = &vmufat_super_operations;

	/* 
	 * Hardware VMUs are 256 blocks in size but
	 * the specification allows for other sizes
	 */
	test_sz = vmufat_get_size(sb, &bh);
	if (test_sz < VMUFAT_MIN_BLK) {
		printk(KERN_ERR "VMUFAT: attempted to mount corrupted vmufat "
			"or non-vmufat violume as vmufat.\n");
		ret = test_sz;
		goto out;
	}

	vmudata = kmalloc(sizeof(struct memcard), GFP_KERNEL);
	if (!vmudata) {
		ret = -ENOMEM;
		goto freebh_out;
	}
	vmufat_populate_vmudata(vmudata, bh, test_sz);
	sema_init(&vmudata->vmu_sem, 1);
	sb->s_fs_info = vmudata;

	root_i = vmufat_get_inode(sb, vmudata->sb_bnum);
	if (!root_i) {
		printk(KERN_ERR "VMUFAT: get root inode failed\n");
		ret = -ENOMEM;
		goto freevmudata_out;
	}
	if (IS_ERR(root_i)) {
		printk(KERN_ERR "VMUFAT: get root"
			" inode failed - error 0x%lX\n",
			PTR_ERR(root_i));
		ret = PTR_ERR(root_i);
		goto freevmudata_out;
	}

	sb->s_root = d_alloc_root(root_i);

	if (!sb->s_root) {
		ret = -EIO;
		goto freeroot_out;
	}
	return 0;

freeroot_out:
	iput(root_i);
freevmudata_out:
	kfree(vmudata);
freebh_out:
	brelse(bh);
out:
	return ret;
}

static void init_once(void *foo)
{
	struct vmufat_inode *vi = foo;

	vi->nblcks = 0;
	inode_init_once(&vi->vfs_inode);
}

static int init_inodecache(void)
{
	vmufat_inode_cachep = kmem_cache_create("vmufat_inode_cache",
		sizeof(struct vmufat_inode), 0,
			SLAB_RECLAIM_ACCOUNT|SLAB_MEM_SPREAD, init_once);
	if (!vmufat_inode_cachep)
		return -ENOMEM;

	vmufat_blist_cachep = kmem_cache_create("vmufat_blocklist_cache",
		sizeof(struct vmufat_block_list), 0, SLAB_MEM_SPREAD, NULL);
	if (!vmufat_blist_cachep) {
		kmem_cache_destroy(vmufat_inode_cachep);
		return -ENOMEM;
	}
	return 0;
}

static void destroy_inodecache(void)
{
	kmem_cache_destroy(vmufat_blist_cachep);
	kmem_cache_destroy(vmufat_inode_cachep);
}

static const struct super_operations vmufat_super_operations = {
	.alloc_inode =		vmufat_alloc_inode,
	.destroy_inode =	vmufat_destroy_inode,
	.write_inode =		vmufat_write_inode,
	.evict_inode =		vmufat_evict_inode,
	.put_super =		vmufat_put_super,
	.statfs =		vmufat_statfs,
};

static struct dentry *vmufat_mount(struct file_system_type *fs_type,
	int flags, const char *dev_name, void *data)
{
	return mount_bdev(fs_type, flags, dev_name, data, vmufat_fill_super);
}

static struct file_system_type vmufat_fs_type = {
	.owner		= THIS_MODULE,
	.name		= "vmufat",
	.mount		= vmufat_mount,
	.kill_sb	= kill_block_super,
	.fs_flags	= FS_REQUIRES_DEV,
};

static int __init init_vmufat_fs(void)
{
	int err;
	err = init_inodecache();
	if (err)
		return err;
	return register_filesystem(&vmufat_fs_type);
}

static void __exit exit_vmufat_fs(void)
{
	destroy_inodecache();
	unregister_filesystem(&vmufat_fs_type);
}

module_init(init_vmufat_fs);
module_exit(exit_vmufat_fs);

MODULE_DESCRIPTION("Filesystem used in Sega Dreamcast VMU");
MODULE_AUTHOR("Adrian McMenamin <adrianmcmenamin@gmail.com>");
MODULE_LICENSE("GPL");
