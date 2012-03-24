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

const struct inode_operations vmufat_inode_operations;
const struct file_operations vmufat_file_operations;
const struct address_space_operations vmufat_address_space_operations;
const struct file_operations vmufat_file_dir_operations;
struct kmem_cache *vmufat_blist_cachep;
/* Linear day numbers of the respective 1sts in non-leap years. */
int day_n[] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};

static struct dentry *vmufat_inode_lookup(struct inode *in, struct dentry *dent,
	struct nameidata *ignored)
{
	struct super_block *sb;
	struct memcard *vmudetails;
	struct buffer_head *bh = NULL;
	struct inode *ino;
	int i, j, error = 0;

	if (dent->d_name.len > VMUFAT_NAMELEN) {
		error = -ENAMETOOLONG;
		goto out;
	}
	sb = in->i_sb;
	vmudetails = sb->s_fs_info;

	for (i = vmudetails->dir_bnum;
		i > vmudetails->dir_bnum - vmudetails->dir_len; i--) {
		brelse(bh);
		bh = vmufat_sb_bread(sb, i);
		if (!bh) {
			error = -EIO;
			goto out;
		}
		for (j = 0; j < VMU_DIR_ENTRIES_PER_BLOCK; j++) {
			int record_offset = j * VMU_DIR_RECORD_LEN;
			if (bh->b_data[record_offset] == 0)
				goto fail;
			if (memcmp(dent->d_name.name,
			bh->b_data + record_offset + VMUFAT_NAME_OFFSET,
			dent->d_name.len) == 0) {
				ino = vmufat_get_inode(sb,
					le16_to_cpu(((u16 *) bh->b_data)
					[record_offset
					+ VMUFAT_FIRSTBLOCK_OFFSET16]));
				if (IS_ERR(ino)) {
					error = PTR_ERR(ino);
					goto out;
				}
				else if (!ino) {
					error = -EACCES;
					goto out;
				}
				d_add(dent, ino);
				goto out;
			}
		}
	}
fail:
	d_add(dent, NULL); /* Did not find the file */
out:
	brelse(bh);
	return ERR_PTR(error);
}

static int vmufat_get_freeblock(int start, int end, struct buffer_head *bh)
{
	int i, ret = -1;
	__le16 fatdata;

	for (i = start; i >= end; i--) {
		fatdata = le16_to_cpu(((u16 *)bh->b_data)[i]);
		if (fatdata == VMUFAT_UNALLOCATED) {
			ret = i;
			break;
		}
	}
	return ret;
}	

/*
 * Find a block marked free in the FAT
 */
static int vmufat_find_free(struct super_block *sb)
{
	struct memcard *vmudetails;
	int testblk, fatblk, ret;
	struct buffer_head *bh_fat;

	vmudetails = sb->s_fs_info;

	for (fatblk = vmudetails->fat_bnum;
		fatblk > vmudetails->fat_bnum - vmudetails->fat_len;
		fatblk--) {
		bh_fat = vmufat_sb_bread(sb, fatblk);
		if (!bh_fat) {
			ret = -EIO;
			goto fail;
		}

		/* Handle 256 block VMUs like physical devices
		 * and other VMUs more simply
		 */
		if (vmudetails->sb_bnum != VMU_BLK_SZ16) {
			/* Cannot be physical VMU */
			testblk = vmufat_get_freeblock(VMU_BLK_SZ16, 0, bh_fat);
			put_bh(bh_fat);
			if (testblk >= 0) 
				goto out_of_loop;
		} else { /* Physical VMU or logical VMU with same size */
			testblk = vmufat_get_freeblock(VMUFAT_START_ALLOC, 0,
				bh_fat);
			if (testblk >= 0) {
				put_bh(bh_fat);
				goto out_of_loop;
			}
			/* Only allocate to higher blocks if no space left */
			testblk = vmufat_get_freeblock(VMU_BLK_SZ16,
				VMUFAT_START_ALLOC + 1, bh_fat);
			put_bh(bh_fat);
			if (testblk > VMUFAT_START_ALLOC)
				goto out_of_loop;
		}
	}
	printk(KERN_INFO "VMUFAT: volume is full\n");
	ret = -ENOSPC;
	goto fail;

out_of_loop:
	ret = (fatblk - 1 - vmudetails->fat_bnum + vmudetails->fat_len)
			* VMU_BLK_SZ16 + testblk;
fail:
	return ret;
}

/* read the FAT for a given block */
u16 vmufat_get_fat(struct super_block *sb, long block)
{
	struct buffer_head *bufhead;
	int offset;
	u16 block_content = VMUFAT_ERROR;
	struct memcard *vmudetails;
	
	vmudetails = sb->s_fs_info;

	/* which block in the FAT */
	offset = block / VMU_BLK_SZ16;
	if (offset >= vmudetails->fat_len)
		goto out;

	/* fat_bnum points to highest block in FAT */
	bufhead = vmufat_sb_bread(sb, offset + 1 +
		vmudetails->fat_bnum - vmudetails->fat_len);
	if (!bufhead)
		goto out;
	/* look inside the block */
	block_content = le16_to_cpu(((u16 *)bufhead->b_data)
		[block % VMU_BLK_SZ16]);
	put_bh(bufhead);
out:
	return block_content;
}

/* set the FAT for a given block */
static int vmufat_set_fat(struct super_block *sb, long block,
		u16 data_to_set)
{
	struct buffer_head *bh;
	int offset, error = 0;
	struct memcard *vmudetails;
	
	vmudetails = sb->s_fs_info;

	offset = block / VMU_BLK_SZ16;
	if (offset >= vmudetails->fat_len) {
		error = -EINVAL;
		goto out;
	}
	bh = vmufat_sb_bread(sb, offset + 1 +
		vmudetails->fat_bnum - vmudetails->fat_len);
	if (!bh) {
		error = -EIO;
		goto out;
	}
	((u16 *) bh->b_data)[block % VMU_BLK_SZ16] = cpu_to_le16(data_to_set);
	mark_buffer_dirty(bh);
	put_bh(bh);
out:
	return error;
}


static void vmufat_save_bcd_nortc(struct inode *in, char *bh, int index_to_dir)
{
	long years, days;
	unsigned char bcd_century, nl_day, bcd_month;
	unsigned char u8year;
	__kernel_time_t unix_date;

	unix_date = in->i_mtime.tv_sec;
	days = unix_date / SECONDS_PER_DAY;
	years = days / DAYS_PER_YEAR;
	/* 1 Jan gets 1 day later after every leap year */
	if ((years + 3) / 4 + DAYS_PER_YEAR * years >= days)
		years--;
	/* rebase days to account for leap years */
	days -= (years + 3) / 4 + DAYS_PER_YEAR * years;
	/* 1 Jan is Day 1 */
	days++;
	if (days == (FEB28 + 1) && !(years % 4)) {
		nl_day = days;
		bcd_month = 2;
	} else {
		nl_day = (years % 4) || days <= FEB28 ? days : days - 1;
		for (bcd_month = 0; bcd_month < 12; bcd_month++)
			if (day_n[bcd_month] > nl_day)
				break;
	}

	bcd_century = 19;
	/* TODO:accounts for 21st century but will fail in 2100
		because of leap days */
	if (years > 29)
		bcd_century += 1 + (years - 30)/100;

	bh[index_to_dir + VMUFAT_DIR_CENT] = bin2bcd(bcd_century);
	u8year = years + 70; /* account for epoch */
	if (u8year > 99)
		u8year = u8year - 100;

	bh[index_to_dir + VMUFAT_DIR_YEAR] = bin2bcd(u8year);
	bh[index_to_dir + VMUFAT_DIR_MONTH] = bin2bcd(bcd_month);
	bh[index_to_dir + VMUFAT_DIR_DAY] =
	    bin2bcd(days - day_n[bcd_month - 1]);
	bh[index_to_dir + VMUFAT_DIR_HOUR] =
	    bin2bcd((unix_date / SECONDS_PER_HOUR) % HOURS_PER_DAY);
	bh[index_to_dir + VMUFAT_DIR_MIN] =
		bin2bcd((unix_date / SIXTY_MINS_OR_SECS)
		 % SIXTY_MINS_OR_SECS);
	bh[index_to_dir + VMUFAT_DIR_SEC] =
		bin2bcd(unix_date % SIXTY_MINS_OR_SECS);
}

static void vmufat_save_bcd_rtc(struct rtc_device *rtc, struct inode *in,
	char *bh, int index_to_dir)
{
	struct rtc_time now;

	if (rtc_read_time(rtc, &now) < 0)
		vmufat_save_bcd_nortc(in, bh, index_to_dir);
	bh[index_to_dir + VMUFAT_DIR_CENT] = bin2bcd((char)(now.tm_year/100));
	bh[index_to_dir + VMUFAT_DIR_YEAR] = bin2bcd((char)(now.tm_year % 100));
	bh[index_to_dir + VMUFAT_DIR_MONTH] = bin2bcd((char)(now.tm_mon));
	bh[index_to_dir + VMUFAT_DIR_DAY] = bin2bcd((char)(now.tm_mday));
	bh[index_to_dir + VMUFAT_DIR_HOUR] = bin2bcd((char)(now.tm_hour));
	bh[index_to_dir + VMUFAT_DIR_MIN] = bin2bcd((char)(now.tm_min));
	bh[index_to_dir + VMUFAT_DIR_SEC] = bin2bcd((char)(now.tm_sec));
	bh[index_to_dir + VMUFAT_DIR_DOW] = bin2bcd((char)(now.tm_wday));
}

/*
 * write out the date in bcd format
 * in the appropriate part of the
 * directory entry
 */
void vmufat_save_bcd(struct inode *in, char *bh, int index_to_dir)
{
	struct rtc_device *rtc;
	rtc = rtc_class_open("rtc0");
	if (!rtc)
		vmufat_save_bcd_nortc(in, bh, index_to_dir);
	else {
		vmufat_save_bcd_rtc(rtc, in, bh, index_to_dir);
		rtc_class_close(rtc);
	}
}

static int vmufat_allocate_inode(umode_t imode,
		struct super_block *sb, struct inode *in)
{
	int error = 0;
	/* Executable files must be at the start of the volume */
	if (imode & EXEC) {
		in->i_ino = VMUFAT_ZEROBLOCK;
		if (vmufat_get_fat(sb, 0) != VMUFAT_UNALLOCATED) {
			printk(KERN_INFO "VMUFAT: cannot write excutable "
				"file. Volume block 0 already allocated.\n");
			error = -ENOSPC;
			goto out;
		}
	} else {
		error = vmufat_find_free(sb);
		if (error >= 0)
			in->i_ino = error;
	}
out:
	return error;
}

static void vmufat_setup_inode(struct inode *in, umode_t imode,
		struct super_block *sb)
{
	in->i_uid = current_fsuid();
	in->i_gid = current_fsgid();
	in->i_mtime = in->i_atime = in->i_ctime = CURRENT_TIME;
	in->i_mode = imode;
	in->i_blocks = 1;
	in->i_sb = sb;
	insert_inode_hash(in);
	in->i_op = &vmufat_inode_operations;
	in->i_fop = &vmufat_file_operations;
	in->i_mapping->a_ops = &vmufat_address_space_operations;
}

static void vmu_handle_zeroblock(int recno, struct buffer_head *bh, int ino)
{
	/* offset and header offset settings */
	if (ino != VMUFAT_ZEROBLOCK) {
		((u16 *) bh->b_data)[recno + VMUFAT_START_OFFSET16] =
			cpu_to_le16(ino);
		((u16 *) bh->b_data)[recno + VMUFAT_HEADER_OFFSET16] = 0;
	} else {
		((u16 *) bh->b_data)[recno + VMUFAT_START_OFFSET16] = 0;
		((u16 *) bh->b_data)[recno + VMUFAT_HEADER_OFFSET16] =
			cpu_to_le16(1);
	}
}

static void vmu_write_name(int recno, struct buffer_head *bh, char *name,
	int len)
{
	memset((char *) (bh->b_data + recno + VMUFAT_NAME_OFFSET), '\0',
		VMUFAT_NAMELEN);
	memcpy((char *) (bh->b_data + recno + VMUFAT_NAME_OFFSET),
		name, len);
}

static int vmufat_inode_create(struct inode *dir, struct dentry *de,
		umode_t imode, struct nameidata *nd)
{
	int i, j, entry, found = 0, error = 0, freeblock;
	struct inode *inode;
	struct super_block *sb;
	struct memcard *vmudetails;
	struct buffer_head *bh = NULL;

	sb = dir->i_sb;
	vmudetails = sb->s_fs_info;
	inode = new_inode(sb);
	if (!inode) {
		error = -ENOSPC;
		goto out;
	}

	mutex_lock(&vmudetails->mutex);
	freeblock = vmufat_allocate_inode(imode, sb, inode);
	if (freeblock < 0) {
		mutex_unlock(&vmudetails->mutex);
		error = freeblock;
		goto clean_inode;
	}
	/* mark as single block file - may grow later */
	error = vmufat_set_fat(sb, freeblock, VMUFAT_FILE_END);
	mutex_unlock(&vmudetails->mutex);
	if (error)
		goto clean_inode;

	vmufat_setup_inode(inode, imode, sb);

	/* Write to the directory
	 * Now search for space for the directory entry */
	mutex_lock(&vmudetails->mutex);
	for (i = vmudetails->dir_bnum;
		i > vmudetails->dir_bnum - vmudetails->dir_len; i--) {
		brelse(bh);
		bh = vmufat_sb_bread(sb, i);
		if (!bh) {
			mutex_unlock(&vmudetails->mutex);
			error = -EIO;
			goto clean_fat;
		}
		for (j = 0; j < VMU_DIR_ENTRIES_PER_BLOCK; j++) {
			entry = j * VMU_DIR_RECORD_LEN;
			if (((bh->b_data)[entry]) == 0) {
				found = 1;
				goto dir_space_found;
			}
		}
	}
	if (found == 0)
		goto clean_fat;
dir_space_found:
	/* Have the directory entry
	 * so now update it */
	if (imode & EXEC)
		bh->b_data[entry] = VMU_GAME;
	else
		bh->b_data[entry] = VMU_DATA;

	/* copy protection settings */
	if (bh->b_data[entry + 1] != (char) NOCOPY)
		bh->b_data[entry + 1] = (char) CANCOPY;

	vmu_handle_zeroblock(entry / 2, bh, inode->i_ino);
	vmu_write_name(entry, bh, (char *) de->d_name.name, de->d_name.len);

	/* BCD timestamp it */
	vmufat_save_bcd(inode, bh->b_data, entry);

	((u16 *) bh->b_data)[entry / 2 + VMUFAT_SIZE_OFFSET16] =
	    cpu_to_le16(inode->i_blocks);
	mark_buffer_dirty(bh);
	brelse(bh);
	error = vmufat_list_blocks(inode);
	if (error)
		goto clean_fat;
	mutex_unlock(&vmudetails->mutex);
	d_instantiate(de, inode);
	return error;

clean_fat:
	vmufat_set_fat(sb, freeblock, VMUFAT_UNALLOCATED);
	mutex_unlock(&vmudetails->mutex);
clean_inode:
	iput(inode);
out:
	if (error < 0)
		printk(KERN_INFO "VMUFAT: inode creation fails with error"
			" %i\n", error);
	return error;
}

static int vmufat_readdir(struct file *filp, void *dirent, filldir_t filldir)
{
	int filenamelen, index, j, k, error = 0;
	struct vmufat_file_info *saved_file = NULL;
	struct dentry *dentry;
	struct inode *inode;
	struct super_block *sb;
	struct memcard *vmudetails;
	struct buffer_head *bh = NULL;

	dentry = filp->f_dentry;
	inode = dentry->d_inode;
	sb = inode->i_sb;
	vmudetails = sb->s_fs_info;
	index = filp->f_pos;
	/* handle . for this directory and .. for parent */
	switch ((unsigned int) filp->f_pos) {
	case 0:
		error = filldir(dirent, ".", 1, index++, inode->i_ino, DT_DIR);
		if (error < 0)
			goto out;
	case 1:
		error = filldir(dirent, "..", 2, index++,
			    dentry->d_parent->d_inode->i_ino, DT_DIR);
		if (error < 0)
			goto out;
	default:
		break;
	}

	saved_file =
	    kmalloc(sizeof(struct vmufat_file_info), GFP_KERNEL);
	if (!saved_file) {
		error = -ENOMEM;
		goto out;
	}

	for (j = vmudetails->dir_bnum -
		(index - 2) / VMU_DIR_ENTRIES_PER_BLOCK;
		j > vmudetails->dir_bnum - vmudetails->dir_len; j--) {
		brelse(bh);
		bh = vmufat_sb_bread(sb, j);
		if (!bh) {
			error = -EIO;
			goto finish;
		}
		for (k = (index - 2) % VMU_DIR_ENTRIES_PER_BLOCK;
			k < VMU_DIR_ENTRIES_PER_BLOCK; k++) {
			int pos, pos16;
			pos = k * VMU_DIR_RECORD_LEN;
			pos16 = k * VMU_DIR_RECORD_LEN16;
			saved_file->ftype = bh->b_data[pos];
			if (saved_file->ftype == 0)
				goto finish;
			saved_file->fblk =
				le16_to_cpu(((u16 *) bh->b_data)[pos16 + 1]);
			if (saved_file->fblk == 0)
				saved_file->fblk = VMUFAT_ZEROBLOCK;
			memcpy(saved_file->fname,
				bh->b_data + pos + VMUFAT_NAME_OFFSET,
				VMUFAT_NAMELEN);
			filenamelen = strlen(saved_file->fname);
			if (filenamelen > VMUFAT_NAMELEN)
				filenamelen = VMUFAT_NAMELEN;
			error = filldir(dirent, saved_file->fname, filenamelen,
				index++, saved_file->fblk, DT_REG);
			if (error < 0)
				goto finish;
		}
	}

finish:
	filp->f_pos = index;
	kfree(saved_file);
	brelse(bh);
out:
	return error;
}


int vmufat_list_blocks(struct inode *in)
{
	struct vmufat_inode *vi;
	struct super_block *sb;
	long nextblock;
	long ino;
	struct memcard *vmudetails;
	int error = -EINVAL;
	struct list_head *iter, *iter2;
	struct vmufat_block_list *vbl, *nvbl;
	u16 fatdata;

	vi = VMUFAT_I(in);
	if (!vi)
		goto out;
	sb = in->i_sb;
	ino = in->i_ino;
	vmudetails = sb->s_fs_info;
	error = 0;
	nextblock = ino;
	if (nextblock == VMUFAT_ZEROBLOCK)
		nextblock = 0;

	/* Delete any previous list of blocks */
	list_for_each_safe(iter, iter2, &vi->blocks.b_list) {
		vbl = list_entry(iter, struct vmufat_block_list, b_list);
		list_del(iter);
		kmem_cache_free(vmufat_blist_cachep, vbl);
	}
	vi->nblcks = 0;
	do {
		vbl = kmem_cache_alloc(vmufat_blist_cachep,
			GFP_KERNEL);
		if (!vbl) {
			error = -ENOMEM;
			goto unwind_out;
		}
		INIT_LIST_HEAD(&vbl->b_list);
		vbl->bno = nextblock;
		list_add_tail(&vbl->b_list, &vi->blocks.b_list);
		vi->nblcks++;

		/* Find next block in the FAT - if there is one */
		fatdata = vmufat_get_fat(sb, nextblock);
		if (fatdata == VMUFAT_UNALLOCATED) {
			printk(KERN_ERR "VMUFAT: FAT table appears to have"
				" been corrupted.\n");
			error = -EIO;
			goto unwind_out;
		}
		if (fatdata == VMUFAT_FILE_END)
			break;
		nextblock = fatdata;
	} while (1);
out:
	return error;

unwind_out:
	list_for_each_entry_safe(vbl, nvbl, &vi->blocks.b_list, b_list) {
		list_del_init(&vbl->b_list);
		kmem_cache_free(vmufat_blist_cachep, vbl);
	}
	return error;
}

static int vmufat_clean_fat(struct super_block *sb, int inum)
{
	int error = 0;
	u16 fatword, nextword;

	nextword = inum;
	do {
		fatword = vmufat_get_fat(sb, nextword);
		if (fatword == VMUFAT_ERROR) {
			error = -EIO;
			break;
		}
		error = vmufat_set_fat(sb, nextword, VMUFAT_UNALLOCATED);
		if (error)
			break;
		if (fatword == VMUFAT_FILE_END)
			break;
		nextword = fatword;
	} while (1);

	return error;
}

/*
 * Delete inode by marking space as free in FAT
 * no need to waste time and effort by actually
 * wiping underlying data on media
 */
static void vmufat_remove_inode(struct inode *in)
{
	struct buffer_head *bh = NULL, *bh_old = NULL;
	struct super_block *sb;
	struct memcard *vmudetails;
	int i, j, k, l, startpt, found = 0;

	if (in->i_ino == VMUFAT_ZEROBLOCK)
		in->i_ino = 0;
	sb = in->i_sb;
	vmudetails = sb->s_fs_info;
	if (in->i_ino > vmudetails->fat_len * sb->s_blocksize / 2) {
		printk(KERN_WARNING "VMUFAT: attempting to delete"
			"inode beyond device size");
		goto ret;
	}

	mutex_lock(&vmudetails->mutex);
	if (vmufat_clean_fat(sb, in->i_ino)) {
		mutex_unlock(&vmudetails->mutex);
		goto failure;
	}

	/* Now clean the directory entry
	 * Have to wander through this
	 * to find the appropriate entry */
	for (i = vmudetails->dir_bnum;
		i > vmudetails->dir_bnum - vmudetails->dir_len; i--) {
		brelse(bh);
		bh = vmufat_sb_bread(sb, i);
		if (!bh) {
			mutex_unlock(&vmudetails->mutex);
			goto failure;
		}
		for (j = 0; j < VMU_DIR_ENTRIES_PER_BLOCK; j++) {
			if (bh->b_data[j * VMU_DIR_RECORD_LEN] == 0) {
				mutex_unlock(&vmudetails->mutex);
				goto failure;
			}
			if (le16_to_cpu(((u16 *) bh->b_data)
				[j * VMU_DIR_RECORD_LEN16 +
				VMUFAT_FIRSTBLOCK_OFFSET16]) == in->i_ino) {
				found = 1;
				goto found;
			}
		}
	}
found:
	if (found == 0) {
		mutex_unlock(&vmudetails->mutex);
		goto failure;
	}

	/* Found directory entry - so NULL it now */
	for (k = 0; k < VMU_DIR_RECORD_LEN; k++)
		bh->b_data[j * VMU_DIR_RECORD_LEN + k] = 0;
	mark_buffer_dirty(bh);
	/* Patch up directory, by moving up last file */
	found = 0;
	startpt = j + 1;
	for (l = i; l > vmudetails->dir_bnum - vmudetails->dir_len; l--) {
		bh_old = vmufat_sb_bread(sb, l);
		if (!bh_old) {
			mutex_unlock(&vmudetails->mutex);
			goto failure;
		}
		for (k = startpt; k < VMU_DIR_ENTRIES_PER_BLOCK; k++) {
			if (bh_old->b_data[k * VMU_DIR_RECORD_LEN] == 0) {
				found = 1;
				brelse(bh_old);
				goto lastdirfound;
			}
		}
		startpt = 0;
		brelse(bh_old);
	}
lastdirfound:
	if (found == 0) {	/* full directory */
		l = vmudetails->dir_bnum - vmudetails->dir_len + 1;
		k = VMU_DIR_ENTRIES_PER_BLOCK;
	} else if (l == i && k == j + 1) /* deleted entry was last in dir */
		goto finish;
	else if (k == 0) {
		l = l + 1;
		k = VMU_DIR_ENTRIES_PER_BLOCK;
		if (l == i && k == j + 1)
			goto finish;
	}
	/* fill gap first then wipe out old entry */
	bh_old = vmufat_sb_bread(sb, l);
	if (!bh_old) {
		mutex_unlock(&vmudetails->mutex);
		brelse(bh);
		goto failure;
	}
	for (i = 0; i < VMU_DIR_RECORD_LEN; i++) {
		bh->b_data[j * VMU_DIR_RECORD_LEN + i] =
			bh_old->b_data[(k - 1) * VMU_DIR_RECORD_LEN + i];
		bh_old->b_data[(k - 1) * VMU_DIR_RECORD_LEN + i] = 0;
	}
	mark_buffer_dirty(bh_old);
	mark_buffer_dirty(bh);
	brelse(bh_old);

finish:
	mutex_unlock(&vmudetails->mutex);
	brelse(bh);
	return;

failure:
	printk(KERN_WARNING "VMUFAT: Failure to read volume,"
		" could not delete inode - filesystem may be damaged\n");
ret:
	return;
}

/*
 * vmufat_unlink - delete a file pointed to
 * by the dentry (only one directory in a
 * vmufat fs so safe to ignore the inode
 * supplied here
 */
static int vmufat_unlink(struct inode *dir, struct dentry *dentry)
{
	struct inode *in;
	in = dentry->d_inode;
	vmufat_remove_inode(in);
	return 0;
}

static int vmufat_get_block(struct inode *inode, sector_t iblock,
	struct buffer_head *bh_result, int create)
{
	struct vmufat_inode *vin;
	struct vmufat_block_list *vlist, *vblk;
	struct super_block *sb;
	struct memcard *vmudetails;
	int cural;
	int finblk, nxtblk, exeblk;
	struct list_head *iter;
	sector_t cntdwn = iblock;
	sector_t phys;
	int error = -EINVAL;

	vin = VMUFAT_I(inode);
	if (!vin || !(&vin->blocks) || vin->nblcks <= 0)
		goto out;
	vlist = &vin->blocks;
	sb = inode->i_sb;
	vmudetails = sb->s_fs_info;

	if (iblock < vin->nblcks) {
		/* block is already here so read it into the buffer head */
		list_for_each(iter, &vlist->b_list) {
			if (cntdwn-- == 0)
				break;
		}
		vblk = list_entry(iter, struct vmufat_block_list, b_list);
		clear_buffer_new(bh_result);
		error = 0;
		phys = vblk->bno;
		goto got_it;
	}
	if (!create)
		goto out;
	/*
	 * check not looking for a block too far
	 * beyond the end of the existing file
	 */
	if (iblock > vin->nblcks)
		goto out;

	/* if looking for a block that is not current - allocate it */
	cural = vin->nblcks;
	list_for_each(iter, &vlist->b_list) {
		if (cural-- == 1)
			break;
	}
	vblk = list_entry(iter, struct vmufat_block_list, b_list);
	finblk = vblk->bno;

	mutex_lock(&vmudetails->mutex);
	/* Exec files have to be linear */
	if (inode->i_ino == 0) {
		exeblk = vmufat_get_fat(sb, finblk + 1);
		if (exeblk != VMUFAT_UNALLOCATED) {
			mutex_unlock(&vmudetails->mutex);
			printk(KERN_WARNING "VMUFAT: Cannot allocate linear "
				"space needed for executible\n");
			error = -ENOSPC;
			goto out;
		}
		nxtblk = finblk + 1;
	} else {
		nxtblk = vmufat_find_free(sb);
		if (nxtblk < 0) {
			mutex_unlock(&vmudetails->mutex);
			error = nxtblk;
			goto out;
		}
	}
	error = vmufat_set_fat(sb, finblk, nxtblk);
	if (error) {
		mutex_unlock(&vmudetails->mutex);
		goto out;
	}
	error = vmufat_set_fat(sb, nxtblk, VMUFAT_FILE_END);
	mutex_unlock(&vmudetails->mutex);
	if (error)
		goto out;
	error = vmufat_list_blocks(inode);
	mark_inode_dirty(inode);
	if (error)
		goto out;
	set_buffer_new(bh_result);
	phys = nxtblk;
	error = 0;
got_it:
	map_bh(bh_result, sb, phys);
out:
	return error;
}

static int vmufat_writepage(struct page *page, struct writeback_control *wbc)
{
	return block_write_full_page(page, vmufat_get_block, wbc);
}

static int vmufat_write_begin(struct file *filp, struct address_space *mapping,
		loff_t pos, unsigned len, unsigned flags,
		struct page **pagep, void **fsdata)
{
	*pagep = NULL;
	return block_write_begin(mapping, pos, len, flags, pagep,
		vmufat_get_block);
}

static int vmufat_readpage(struct file *file, struct page *page)
{
	return block_read_full_page(page, vmufat_get_block);
}


const struct address_space_operations
		vmufat_address_space_operations = {
	.readpage =	vmufat_readpage,
	.writepage =	vmufat_writepage,
	.write_begin =	vmufat_write_begin,
	.write_end =	generic_write_end,
};

const struct inode_operations vmufat_inode_operations = {
	.lookup =	vmufat_inode_lookup,
	.create =	vmufat_inode_create,
	.unlink =	vmufat_unlink,
};

const struct file_operations vmufat_file_dir_operations = {
	.owner =	THIS_MODULE,
	.read =		generic_read_dir,
	.readdir =	vmufat_readdir,
	.fsync =	generic_file_fsync,
};

const struct file_operations vmufat_file_operations = {
	.llseek =	generic_file_llseek,
	.read =		do_sync_read,
	.write =	do_sync_write,
	.aio_read =	generic_file_aio_read,
	.aio_write =	generic_file_aio_write,
	.fsync =	generic_file_fsync,
};
