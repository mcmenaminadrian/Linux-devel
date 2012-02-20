/*
 * inode operations for the VMUFAT file system
 *
 * Copyright (C) 2002 - 2012	Adrian McMenamin
 * Copyright (C) 2002		Paul Mundt
 *
 * Released under the terms of the GNU GPL.
 */
#include <linux/fs.h>
#include <linux/bcd.h>
#include <linux/slab.h>
#include <linux/magic.h>
#include <linux/module.h>
#include <linux/statfs.h>
#include <linux/buffer_head.h>
#include "vmufat.h"

static struct dentry *vmufat_inode_lookup(struct inode *in, struct dentry *dent,
	struct nameidata *ignored)
{
	struct super_block *sb;
	struct memcard *vmudetails;
	struct buffer_head *bufhead = NULL;
	struct inode *ino;
	char name[VMUFAT_NAMELEN];
	int i, j, error = -EINVAL;
printk("In vmufat_inode_lookup\n");
	if (!dent || !in || !in->i_sb) 
		goto out;
	if (dent->d_name.len > VMUFAT_NAMELEN) {
		error = -ENAMETOOLONG;
		goto out;
	}
	sb = in->i_sb;
	if (!sb->s_fs_info)
		goto out;
	vmudetails = sb->s_fs_info;
	error = 0;

	for (i = vmudetails->dir_bnum;
		i > vmudetails->dir_bnum - vmudetails->dir_len; i--)
	{
		brelse(bufhead);
		bufhead = vmufat_sb_bread(sb, i);
		if (!bufhead) {
			error = -EIO;
			goto out;
		}
		for (j = 0; j < VMU_DIR_ENTRIES_PER_BLOCK; j++)
		{
			if (bufhead->b_data[j * VMU_DIR_RECORD_LEN] == 0) 
				goto fail;
			/* get name and check for match */
			memcpy(name,
				bufhead->b_data + j * VMU_DIR_RECORD_LEN
				+ VMUFAT_NAME_OFFSET, VMUFAT_NAMELEN);
			if (memcmp(dent->d_name.name, name,
				dent->d_name.len) == 0) {
				ino = vmufat_get_inode(sb,
					le16_to_cpu(((u16 *) bufhead->b_data)
					[j * VMU_DIR_RECORD_LEN16
					+ VMUFAT_FIRSTBLOCK_OFFSET16]));
				if (IS_ERR_OR_NULL(ino)) {
					if (IS_ERR(ino))
						error = PTR_ERR(ino);
					else
						error = -EACCES;
					goto out;
				}
				d_add(dent, ino); printk(KERN_ERR "Found: %s\n", name);
				goto out;
			}
		}
	}
fail:
	d_add(dent, NULL); /* Did not find the file */
out:
	brelse(bufhead); printk(KERN_ERR "Error from inode lookup is %i \n", error);
	if (error)
		return ERR_PTR(error);
	else
		return NULL;
}

/*
 * Find a block marked free in the FAT
 */
static int vmufat_find_free(struct super_block *sb)
{
	struct memcard *vmudetails;
	int found = 0, testblk, fatblk, error, index_to_fat;
	int diff;
	__le16 fatdata;
	struct buffer_head *bh_fat;

	if (!sb || !sb->s_fs_info) {
		error = -EINVAL;
		goto fail;
	}
	vmudetails = sb->s_fs_info;
printk("In vmufat_find_free\n");

	for (fatblk = vmudetails->fat_bnum;
		fatblk > vmudetails->fat_bnum - vmudetails->fat_len;
		fatblk--) {
		bh_fat = vmufat_sb_bread(sb, fatblk);
		if (!bh_fat) {
			error = -EIO;
			goto fail;
		}
		/* Specification allows for more than one FAT block
		 * but need to careful we do not over-write root
		 * block which may be marked as unallocated on new
		 * VMU device
		 */
	
		if (fatblk < vmudetails->fat_bnum)
			index_to_fat = VMU_BLK_SZ16 - 1;
		else
			index_to_fat =
				vmudetails->fat_bnum - vmudetails->fat_len;
		/* prefer not to allocate to higher blocks if we can
		 * to ensure full compatibility with Dreamcast devices */
		diff = index_to_fat - VMUFAT_START_ALLOC;
		for (testblk = index_to_fat; testblk > 0; testblk--) {
			if (testblk - diff < 0)
				diff = -index_to_fat - 1;
			fatdata =
				le16_to_cpu(((u16 *) bh_fat->b_data)
				[testblk - diff]);
			if (fatdata == VMUFAT_UNALLOCATED) {
				found = 1;
				put_bh(bh_fat);
				goto out_of_loop;
			}
		}
		put_bh(bh_fat);
	}
out_of_loop:
	if (found) 
		return (vmudetails->fat_bnum - fatblk) * VMU_BLK_SZ16
			+ testblk - diff;

	printk(KERN_ERR "VMUFAT: volume is full\n");
	error = -ENOSPC;
fail:
	return error;
}

/* read the FAT for a given block */
static u16 vmufat_get_fat(struct super_block *sb, long block)
{
	struct buffer_head *bufhead;
	int offset;
	u16 block_content = VMUFAT_ERROR;
	struct memcard *vmudetails;
printk("In vmufat_get_fat\n");

	if (!sb || !sb->s_fs_info)
		goto out;
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
printk("In vmufat_set_fat\n");
	if (!sb || !sb->s_fs_info) {
		error = -EINVAL;
		goto out;
	}
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
	return 0;
}

/*
 * write out the date in bcd format
 * in the appropriate part of the 
 * directory entry
 */
static void vmufat_save_bcd(struct inode *in, char *bh, int index_to_dir)
{
	long years, days;
	unsigned char bcd_century, nl_day, bcd_month;
	unsigned char u8year;
	__kernel_time_t unix_date = in->i_mtime.tv_sec;

	if (!in || !bh)
		return;

	/* Unix epoch beings 1/1/1970, vmufat date stamps begin 1/1/1980 */
	days = unix_date / SECONDS_PER_DAY - 3652;
	years = days / DAYS_PER_YEAR;

	if ((years + 3) / 4 + DAYS_PER_YEAR * years > days)
		years--;

	days -= (years + 3) / 4 + DAYS_PER_YEAR * years;
	if (days == 59 && !(years & 3)) {
		nl_day = days;
		bcd_month = 2;
	} else {
		nl_day = (years & 3) || days <= FEB28 ? days : days - 1;
		for (bcd_month = 0; bcd_month < 12; bcd_month++)
			if (day_n[bcd_month] > nl_day)
				break;
	}

	bcd_century = 19;
	/* TODO: accounts for 21st century but will fail in 2100 
	 	 because of leap days */
	if (years > 19)
		bcd_century += 1 + (years - 20)/100;

	bh[index_to_dir + VMUFAT_DIR_CENT] = bin2bcd(bcd_century);
	u8year = years + 80; /* account for epoch start */
	if (u8year > 99)
		u8year = u8year - 100;

	bh[index_to_dir + VMUFAT_DIR_YEAR] = bin2bcd(u8year);
	bh[index_to_dir + VMUFAT_DIR_MONTH] = bin2bcd(bcd_month);
	bh[index_to_dir + VMUFAT_DIR_DAY] =
	    bin2bcd(days - day_n[bcd_month - 1] + 1);
	bh[index_to_dir + VMUFAT_DIR_HOUR] =
	    bin2bcd((unix_date / SECONDS_PER_HOUR) % HOURS_PER_DAY);
	bh[index_to_dir + VMUFAT_DIR_MIN] = bin2bcd((unix_date / SIXTY_MINS_OR_SECS)
		 % SIXTY_MINS_OR_SECS);
	bh[index_to_dir + VMUFAT_DIR_SEC] = bin2bcd(unix_date % SIXTY_MINS_OR_SECS);
}

static int vmufat_allocate_inode(umode_t imode, struct super_block *sb, struct inode *in)
{
	int error;
	if (!sb || !in)
		return -EINVAL;
printk("In vmufat_allocate_inode\n");
	/* Executable files must be at the start of the volume */
	if (imode & 0111) {
		in->i_ino = VMUFAT_ZEROBLOCK;
		if (vmufat_get_fat(sb, 0) != VMUFAT_UNALLOCATED) {
			printk("VMUFAT: cannot write excutable file to volume."
				"Block 0 already allocated.\n");
			error = -ENOSPC;
			goto out;
		}
		error = 0;
	} else {
		error = vmufat_find_free(sb);
		if (error >= 0)
			in->i_ino = error;
	}
out:
	return error;
}

static void vmufat_setup_inode(struct inode *in, umode_t imode, struct super_block *sb)
{
	if (!in)
		return;
	in->i_uid = 0;
	in->i_gid = 0;
	in->i_mtime = in->i_atime = in->i_ctime = CURRENT_TIME;
	in->i_mode = imode;
	in->i_blocks = 1;
	in->i_sb = sb;
	insert_inode_hash(in);
	in->i_op = &vmufat_inode_operations;
	in->i_fop = &vmufat_file_operations;
	in->i_mapping->a_ops = &vmufat_address_space_operations;
}

static int vmufat_inode_create(struct inode *dir, struct dentry *de,
		umode_t imode, struct nameidata *nd)
{
	/* Create an inode */
	int i, j, found = 0, error = 0, freeblock;
	struct inode *inode;
	struct super_block *sb;
	struct memcard *vmudetails;
	struct buffer_head *bh = NULL;
printk("In vmufat_inode_create\n");
	if (!dir || !de) {
		error = -EINVAL;
		goto out;
	}

	if (de->d_name.len > VMUFAT_NAMELEN) {
		error = -ENAMETOOLONG;
		goto out;
	}

	sb = dir->i_sb;
	if (!sb || !sb->s_fs_info) {
		error = -EINVAL;
		goto out;
	}
	vmudetails = sb->s_fs_info;

	inode = new_inode(sb);
	if (!inode) {
		error = -ENOSPC;
		goto out;
	}

	down_interruptible(&vmudetails->vmu_sem);
	freeblock = vmufat_allocate_inode(imode, sb, inode);
	if (freeblock < 0) {
		up(&vmudetails->vmu_sem);
		error = freeblock;
		goto clean_inode;
	}
	/* mark as single block file - may grow later */
	error = vmufat_set_fat(sb, freeblock, VMUFAT_FILE_END);
	up(&vmudetails->vmu_sem);
	if (error)
		goto clean_inode;

	vmufat_setup_inode(inode, imode, sb);

	/* Write to the directory
	* Now search for space for the directory entry */
	down_interruptible(&vmudetails->vmu_sem);
	for (i = vmudetails->dir_bnum;
		i > vmudetails->dir_bnum - vmudetails->dir_len; i--)
	{
		brelse(bh);
		bh = vmufat_sb_bread(sb, i);
		if (!bh) {
			up(&vmudetails->vmu_sem);
			error = -EIO;
			goto clean_fat;
		}
		for (j = 0; j < VMU_DIR_ENTRIES_PER_BLOCK; j++)
		{
			if (((bh->b_data)[j * VMU_DIR_RECORD_LEN]) == 0) {
				up(&vmudetails->vmu_sem);
				found = 1;
				goto dir_space_found;
			}
		}
	}
	if (found == 0) 
		goto clean_fat;
dir_space_found:
	j = j * VMU_DIR_RECORD_LEN;
	/* Have the directory entry
	 * so now update it */
	if (imode & 0111)
		bh->b_data[j] = VMU_GAME;	/* exec file */
	else
		bh->b_data[j] = VMU_DATA;

	/* copy protection settings */
	if (bh->b_data[j + 1] != (char) 0xff)
		bh->b_data[j + 1] = (char) 0x00;

	/* offset and header offset settings */
	if (inode->i_ino != VMUFAT_ZEROBLOCK) {
		((u16 *) bh->b_data)[j / 2 + 1] =
		    cpu_to_le16(inode->i_ino);
		((u16 *) bh->b_data)[j / 2 + 0x0D] = 0;
	} else {
		((u16 *) bh->b_data)[j / 2 + 1] = 0;
		((u16 *) bh->b_data)[j / 2 + 0x0D] = 1;
	}

	/* Name */
	memset((char *) (bh->b_data + j + VMUFAT_NAME_OFFSET), '\0',
		VMUFAT_NAMELEN);
	memcpy((char *) (bh->b_data + j + VMUFAT_NAME_OFFSET),
		((de->d_name).name), de->d_name.len);

	/* BCD timestamp it */
	vmufat_save_bcd(inode, bh->b_data, j);

	((u16 *) bh->b_data)[j / 2 + 0x0C] =
	    cpu_to_le16(inode->i_blocks);
	mark_buffer_dirty(bh);
	brelse(bh);

	error = vmufat_list_blocks(inode);
	if (error)
		goto clean_fat;
	up(&vmudetails->vmu_sem);
	d_instantiate(de, inode);
	return error;

clean_fat:
	vmufat_set_fat(sb, freeblock, VMUFAT_UNALLOCATED);
	up(&vmudetails->vmu_sem);
clean_inode:
	iput(inode);
out:
	return error;
}

static int vmufat_readdir(struct file *filp, void *dirent, filldir_t filldir)
{
	int filenamelen, index, j, k, error = -EINVAL;
	struct vmufat_file_info *saved_file = NULL;
	struct dentry *dentry;
	struct inode *inode;
	struct super_block *sb;
	struct memcard *vmudetails;
	struct buffer_head *bh = NULL;
printk("ENTERED\n");
	if (!filp || !filp->f_dentry)
		goto out;
	dentry = filp->f_dentry;
	inode = dentry->d_inode;
	if (!inode)
		goto out;
	sb = inode->i_sb;
	if (!sb)
		goto out;
	vmudetails = sb->s_fs_info;
	if (!vmudetails)
		goto out;
	error = 0;

	index = filp->f_pos; printk("Index reads %i\n", index);
	if (index > 200)
		return -EIO;
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
		j > vmudetails->dir_bnum - vmudetails->dir_len; j--)
	{
		brelse(bh);
		bh = vmufat_sb_bread(sb, j);
		if (!bh) {
			error = -EIO;
			goto finish;
		}
		for (k = (index - 2) % VMU_DIR_ENTRIES_PER_BLOCK;
			k < VMU_DIR_ENTRIES_PER_BLOCK; k++)
		{
			saved_file->ftype = bh->b_data[k * VMU_DIR_RECORD_LEN];
			if (saved_file->ftype == 0)
				goto finish;
			saved_file->fblk =
				le16_to_cpu(((u16 *) bh->b_data)
				[k * VMU_DIR_RECORD_LEN16 + 1]);
			if (saved_file->fblk == 0)
				saved_file->fblk = VMUFAT_ZEROBLOCK;
			memcpy(saved_file->fname,
				bh->b_data + k * VMU_DIR_RECORD_LEN
				+ VMUFAT_NAME_OFFSET, VMUFAT_NAMELEN);
			filenamelen = strlen(saved_file->fname);
			if (filenamelen > VMUFAT_NAMELEN)
				filenamelen = VMUFAT_NAMELEN;
			error = filldir(dirent, saved_file->fname, filenamelen,
				index++, saved_file->fblk, DT_REG);
			if (error < 0)
				goto finish;
			printk("GOT %s, k is %i, j is %i, index is %i\n", saved_file->fname, k, j, index);
		}
	}

finish:
	filp->f_pos = index;
	kfree(saved_file);
	brelse(bh);
out:
	printk(KERN_ALERT"Error is %i\n", error); return error;
}

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

	return 	mktime(century * 100 + year, month, day, hour, minute,
			second);
}

static struct inode *vmufat_alloc_inode(struct super_block *sb)
{
	struct vmufat_inode *vi = kmem_cache_alloc(vmufat_inode_cachep,
		GFP_KERNEL);
printk("In vmufat_alloc_inode\n");
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
printk("In vmufat_destroy_inode\n");
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

static int vmufat_list_blocks(struct inode *in)
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

	if (!in || !in->i_sb || !in->i_ino)
		goto out;
	vi = VMUFAT_I(in);
	if (!vi) 
		goto out;
	sb = in->i_sb;
	ino = in->i_ino; 
	vmudetails = sb->s_fs_info;
	if (!vmudetails)
		goto out;
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
			printk(KERN_WARNING "VMUFAT: FAT table appears to have"
				" been corrupted.\n");
			error = -EIO;
			goto unwind_out;
		}
		if (fatdata == VMUFAT_FILE_END)
			break;	/*end of file */
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

static struct inode *vmufat_get_inode(struct super_block *sb, long ino)
{
	struct buffer_head *bh = NULL;
	int error, i, j, found = 0;
	int offsetindir;
	struct inode *inode;
	struct memcard *vmudetails;
	long superblock_bno;
printk("In vmufat_get_inode\n");
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
		inode->i_uid = 0;
		inode->i_gid = 0;
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
			inode->i_mode = S_IFREG;

			/* Scan through the directory to find matching file */
			for (i = vmudetails->dir_bnum;
				i > vmudetails->dir_bnum - vmudetails->dir_len;
				i--)
			{
				brelse(bh);
				bh = vmufat_sb_bread(sb, i);
				if (!bh) {
					error = -EIO;
					goto failed;
				}
				for (j = 0; j < VMU_DIR_ENTRIES_PER_BLOCK; j++)
				{
					if (bh->b_data
					[j * VMU_DIR_RECORD_LEN] == 0) {
						printk("VMUFAT:"
							" file not found\n");
						error = -ENOENT;
						goto failed;
					}
					if (le16_to_cpu(((u16 *) bh->b_data)
					[j * VMU_DIR_RECORD_LEN16 +
					VMUFAT_FIRSTBLOCK_OFFSET16]) == ino) {
						found = 1;
						goto found;
					}
				}
			}
found:
			if (found == 0)
				goto failed;
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
				inode->i_mode |= S_IWUGO;
			/* Is file executible - ie a game */
			if ((((u8 *) bh->b_data)[offsetindir] ==
			     0xcc) & ~(sb->s_flags & MS_NOEXEC))
				inode->i_mode |= S_IXUGO;

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
	int i, j;
	struct buffer_head *bh_fat;
	struct memcard *vmudetails;

	if (!sb || !kstatbuf) {
		error = -EINVAL;
		goto out;
	}

	vmudetails = sb->s_fs_info;
	if (!vmudetails) {
		error = -EINVAL;
		goto out;
	}

	/* Look through the FAT */
	for (i = vmudetails->fat_bnum;
		i > vmudetails->fat_bnum - vmudetails->fat_len; i--)
	{
		bh_fat = vmufat_sb_bread(sb, i);
		if (!bh_fat) {
			error = -EIO;
			goto out;
		}
		for (j = 0; j < VMU_BLK_SZ16; j++)
		{
			if (vmufat_get_fat(sb, i * VMU_BLK_SZ16 + j) ==
				VMUFAT_UNALLOCATED)
				free++;
		}
		brelse(bh_fat);
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

static int vmufat_clean_fat(struct super_block *sb, int inum)
{
	int error = 0;
	u16 fatword, nextword;
	if (!sb) {
		error = -EINVAL;
		goto out;
	}
	nextword = inum;
	do {
		fatword = vmufat_get_fat(sb, nextword);
		if (fatword == VMUFAT_ERROR) {
			error = -EIO;
			goto out;
		}
		error = vmufat_set_fat(sb, nextword, VMUFAT_UNALLOCATED);
		if (error)
			goto out;
		if (fatword == VMUFAT_FILE_END)
			goto out;
		nextword = fatword;
	} while(1);
out:
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
printk("In vmufat_remove_inode\n");
	if (!in || !in->i_sb)
		goto ret;

	if (in->i_ino == VMUFAT_ZEROBLOCK)
		in->i_ino = 0;
	sb = in->i_sb;
	vmudetails = sb->s_fs_info;
	if (!vmudetails)
		goto ret;
	if (in->i_ino > vmudetails->fat_len * sb->s_blocksize / 2) {
		printk(KERN_ERR "VMUFAT: attempting to delete"
			"inode beyond device size");
		goto ret;
	}

	down_interruptible(&vmudetails->vmu_sem);
	if (vmufat_clean_fat(sb, in->i_ino)) {
		up(&vmudetails->vmu_sem);
		goto failure;
	}

	/* Now clean the directory entry
	 * Have to wander through this
	 * to find the appropriate entry */
	for (i = vmudetails->dir_bnum;
		i > vmudetails->dir_bnum - vmudetails->dir_len; i--)
	{
		brelse(bh);
		bh = vmufat_sb_bread(sb, i);
		if (!bh) {
			up(&vmudetails->vmu_sem);
			goto failure;
		}
		for (j = 0; j < VMU_DIR_ENTRIES_PER_BLOCK; j++)
		{
			if (bh->b_data[j * VMU_DIR_RECORD_LEN] == 0) {
				up(&vmudetails->vmu_sem);
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
		up(&vmudetails->vmu_sem);
		goto failure;
	}

	/* Found directory entry - so NULL it now */
	for (k = 0; k < VMU_DIR_RECORD_LEN; k++)
		bh->b_data[j * VMU_DIR_RECORD_LEN + k] = 0;
	mark_buffer_dirty(bh);
	/* Patch up directory, by moving up last file */
	found = 0;
	startpt = j + 1;
	for (l = i; l > vmudetails->dir_bnum - vmudetails->dir_len; l--)
	{
		bh_old = vmufat_sb_bread(sb, l);
		if (!bh_old) {
			up(&vmudetails->vmu_sem);
			goto failure;
		}
		for (k = startpt; k < VMU_DIR_ENTRIES_PER_BLOCK; k++)
		{
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
	if (found == 0) /* full directory */
	{
		l = vmudetails->dir_bnum - vmudetails->dir_len + 1;
		k = VMU_DIR_ENTRIES_PER_BLOCK;
	}
	else if (l == i && k == j + 1) /* deleted entry was last in dir */
		goto finish; 
	else if (k == 0)
	{
		l = l - 1;
		k = VMU_DIR_ENTRIES_PER_BLOCK;
		if (l == i && k == j + 1)
			goto finish;
	}
	/* fill gap first then wipe out old entry */
	bh_old = vmufat_sb_bread(sb, l);
	if (!bh_old) {
		up(&vmudetails->vmu_sem);
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
	up(&vmudetails->vmu_sem);
	brelse(bh);
	return;

failure:
	printk(KERN_ERR "VMUFAT: Failure to read volume,"
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
	if (!dentry || !dentry->d_inode)
		return -EINVAL;
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
printk("In vmufat_get_block\n");
	if (!inode || !inode->i_sb)
		goto out;
	vin = VMUFAT_I(inode);
	if (!vin || !(&vin->blocks))
		goto out;
	vlist = &vin->blocks;
	sb = inode->i_sb;
	vmudetails = sb->s_fs_info;
	if (!vmudetails)
		goto out;

	/* quick sanity check */
	if (vin->nblcks <= 0)
		goto out;
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

	/* if looking for a block that is not current - allocate it*/
	cural = vin->nblcks;
	list_for_each(iter, &vlist->b_list) {
		if (cural-- == 1)
			break;
	}
	vblk = list_entry(iter, struct vmufat_block_list, b_list);
	finblk = vblk->bno;

	down_interruptible(&vmudetails->vmu_sem);
	/* Exec files have to be linear */
	if (inode->i_ino == 0) {
		exeblk = vmufat_get_fat(sb, finblk + 1);
		if (exeblk != VMUFAT_UNALLOCATED) {
			up(&vmudetails->vmu_sem);
			printk(KERN_WARNING "VMUFAT: Cannot allocate linear "
				"space needed for executible\n");
			error = -ENOSPC;
			goto out;
		}
		nxtblk = finblk + 1;
	} else {
		nxtblk = vmufat_find_free(sb);
		if (nxtblk < 0) {
			up(&vmudetails->vmu_sem);
			error = nxtblk;
			goto out;
		}
	}
	error = vmufat_set_fat(sb, finblk, nxtblk);
	if (error) {
		up(&vmudetails->vmu_sem);
		goto out;
	}
	error = vmufat_set_fat(sb, nxtblk, VMUFAT_FILE_END);
	up(&vmudetails->vmu_sem);
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
printk("In vmufat_write_inode\n");
	if (!in || !in->i_sb || !in->i_ino)
		return -EINVAL;
	sb = in->i_sb;
	vmudetails = sb->s_fs_info;
	if (!vmudetails)
		return -EINVAL;

	/* As most real world devices are flash
 	 * we won't update the superblock every
 	 * time we change something else on the fs
 	 * - it is ugly but a sensible compromise
 	 */
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
		i > vmudetails->dir_bnum - vmudetails->dir_len; i--)
	{
		bh = vmufat_sb_bread(sb, i);
		if (!bh) {
			up(&vmudetails->vmu_sem);
			return -EIO;
		}
		for (j = 0; j < VMU_DIR_ENTRIES_PER_BLOCK; j++)
		{
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
	u32 s_magic = VMUFAT_MAGIC;
	if (!bh)
		return -EINVAL;

	if (!(((u32 *) bh->b_data)[0] == s_magic &&
		((u32 *) bh->b_data)[1] == s_magic &&
		((u32 *) bh->b_data)[2] == s_magic &&
		((u32 *) bh->b_data)[3] == s_magic))
		return 0;
	else
		return 1;
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

static int vmufat_fill_super(struct super_block *sb,
					    void *data, int silent)
{
	/* Search for the superblock */

	struct buffer_head *bh;
	struct memcard *vmudata;
	int test_sz;
	struct inode *root_i;
	int ret = -EINVAL;
printk("In vmufat_fill_super\n");
	if (!sb)
		goto out;

	sb_set_blocksize(sb, VMU_BLK_SZ);

	/* 
	 * Hardware VMUs are 256 blocks in size but
	 * the specification allows for other sizes so
	 * we search through sizes
	 */
	for (test_sz = VMUFAT_MIN_BLK; test_sz < VMUFAT_MAX_BLK;
				test_sz = test_sz * 2) {
		bh = vmufat_sb_bread(sb, test_sz - 1);
		if (!bh) {
			ret = -EIO;
			goto out;
		}
		if (check_sb_format(bh))
			break;
		brelse(bh);
		if (test_sz == VMUFAT_MAX_BLK) {	/* failed */
			printk(KERN_ERR
				"VMUFAT: attempted to mount corrupted vmufat"
				" or non-vmufat volume as vmufat\n");
			goto out;
		}
	}
	/* Store this data in the super block */
	vmudata = kmalloc(sizeof(struct memcard), GFP_KERNEL);
	if (!vmudata) {
		ret = -ENOMEM;
		goto freebh_out;
	}

	/* user blocks */
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
	sema_init(&vmudata->vmu_sem, 1);
	sb->s_fs_info = vmudata;

	sb->s_blocksize_bits = ilog2(VMU_BLK_SZ);
	sb->s_magic = VMUFAT_MAGIC;
	sb->s_op = &vmufat_super_operations;

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

static const struct address_space_operations
		vmufat_address_space_operations = {
	.readpage =	vmufat_readpage,
	.writepage =	vmufat_writepage,
	.write_begin =	vmufat_write_begin,
	.write_end =	generic_write_end,
};

static const struct super_operations vmufat_super_operations = {
	.alloc_inode =		vmufat_alloc_inode,
	.destroy_inode =	vmufat_destroy_inode,
	.write_inode =		vmufat_write_inode,
	.evict_inode =		vmufat_evict_inode,
	.put_super =		vmufat_put_super,
	.statfs =		vmufat_statfs,
};

static const struct inode_operations vmufat_inode_operations = {
	.lookup =	vmufat_inode_lookup,
	.create =	vmufat_inode_create,
	.unlink =	vmufat_unlink,
};

static const struct file_operations vmufat_file_dir_operations = {
	.owner =	THIS_MODULE,
	.read =		generic_read_dir,
	.readdir =	vmufat_readdir,
	.fsync =	generic_file_fsync,
};

static const struct file_operations vmufat_file_operations = {
	.llseek =	generic_file_llseek,
	.read =		do_sync_read,
	.write =	do_sync_write,
	.aio_read =	generic_file_aio_read,
	.aio_write =	generic_file_aio_write,
	.fsync =	generic_file_fsync,
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
