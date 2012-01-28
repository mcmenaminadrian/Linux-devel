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
	struct super_block *superbl;
	struct memcard *vmudetails;
	struct buffer_head *bufhead;
	struct inode *ino;
	char name[VMUFAT_NAMELEN];
	long blck_read;
	int error = 0, fno = 0;

	if (dent->d_name.len > VMUFAT_NAMELEN) {
		error = -ENAMETOOLONG;
		goto out;
	}

	superbl = in->i_sb;
	vmudetails = superbl->s_fs_info;
	blck_read = vmudetails->dir_bnum;

	bufhead = vmufat_sb_bread(superbl, blck_read);
	if (!bufhead) {
		error = -EIO;
		goto out;
	}

	do {
		/* Have we got a file? */
		if (bufhead->b_data[vmufat_index(fno)] == 0)
			goto next;

		/* get file name */
		memcpy(name,
			bufhead->b_data + 4 + vmufat_index(fno),
			VMUFAT_NAMELEN);
		/* do names match ?*/
		if (memcmp(dent->d_name.name, name, dent->d_name.len) == 0) {
			/* read the inode number from the directory */
			ino = vmufat_get_inode(superbl,
				le16_to_cpu(((u16 *) bufhead->b_data)
					[1 + vmufat_index_16(fno)]));
			if (!ino) {
				error = -EACCES;
				goto release_bh;
			}
			if (IS_ERR(ino)) {
				error = PTR_ERR(ino);
				goto release_bh;
			}
			/* return the entry */
			d_add(dent, ino);
			goto release_bh;
		}
next:
		/* did not match, so try the next file */
		fno++;
		/* do we need to move to the next block in the directory? */
		if (fno >= DIR_ENT_PER_BLK) {
			fno = 0;
			blck_read--;
			if (blck_read <=
				vmudetails->dir_bnum - vmudetails->dir_len) {
				d_add(dent, NULL);
				break;
			}
			brelse(bufhead);
			bufhead = vmufat_sb_bread(superbl, blck_read);
			if (!bufhead) {
				error = -EIO;
				goto out;
			}
		}
	} while (1);

release_bh:
	brelse(bufhead);
out:
	return ERR_PTR(error);
}

/*
 * Find a free block in the FAT
 */
static int vmufat_find_free(struct super_block *superbl)
{
	struct memcard *vmudetails = superbl->s_fs_info;
	int found = 0, cnt, fatcnt, error, index_to_fat;
	__le16 fatdata;
	struct buffer_head *bh_fat;

	for (fatcnt = vmudetails->fat_bnum;
		fatcnt > vmudetails->fat_bnum - vmudetails->fat_len;
		fatcnt--) {
		bh_fat = vmufat_sb_bread(superbl, fatcnt);
		if (!bh_fat) {
			error = -EIO;
			goto fail;
		}
		/* Specification allows for more than one FAT block
		 * but need to careful we do not over-write root
		 * block which may be marked as unallocated on new
		 * VMU device
		 */
	
		if (unlikely(fatcnt < vmudetails->fat_bnum))
			index_to_fat = VMU_BLK_SZ / 2 - 1;
		else
			index_to_fat =
				vmudetails->fat_bnum - vmudetails->fat_len;

		for (cnt = index_to_fat; cnt > 0; cnt--) {
			fatdata =
				le16_to_cpu(((u16 *) bh_fat->b_data)[cnt]);
			if (fatdata == FAT_UNALLOCATED) {
				found = 1;
				goto out_of_loop;
			}
		}
		put_bh(bh_fat);
	}
out_of_loop:
	if (found) {
		put_bh(bh_fat);
		return (vmudetails->fat_bnum - fatcnt) * VMU_BLK_SZ / 2 + cnt;
	}

	printk(KERN_ERR "VMUFAT: volume is full\n");
	error = -ENOSPC;
fail:
	return error;
}

/* read the FAT for a given block */
static u16 vmufat_get_fat(struct super_block *superbl, long block)
{
	struct memcard *vmudetails = superbl->s_fs_info;
	struct buffer_head *bufhead;
	int offset;
	u16 block_content;
	/* which block in the FAT */
	offset = block / (VMU_BLK_SZ / 2);
	if (offset >= vmudetails->fat_len)
		return FAT_ERROR;

	bufhead = vmufat_sb_bread(superbl, offset + 1 +
		vmudetails->fat_bnum - vmudetails->fat_len);
	if (!bufhead)
		return FAT_ERROR;
	/* look inside the block */
	block_content = le16_to_cpu(((u16 *)bufhead->b_data)
		[block % (VMU_BLK_SZ / 2)]);
	put_bh(bufhead);
	return block_content;
}

/* set the FAT for a given block */
static int vmufat_set_fat(struct super_block *sb, long block, u16 data_to_set)
{
	struct memcard *vmudetails = sb->s_fs_info;
	struct buffer_head *bh;
	int offset;
	__le16 leset = cpu_to_le16(data_to_set);
	offset = block / (VMU_BLK_SZ / 2);
	if (offset >= vmudetails->fat_len)
		return -EINVAL;

	bh = vmufat_sb_bread(sb, offset + 1 +
		vmudetails->fat_bnum - vmudetails->fat_len);
	if (!bh)
		return -EIO;

	((u16 *) bh->b_data)[block % (VMU_BLK_SZ / 2)] = leset;
	mark_buffer_dirty(bh);
	put_bh(bh);
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

static int vmufat_inode_create(struct inode *dir, struct dentry *de,
		int imode, struct nameidata *nd)
{
	/* Create an inode */
	int y, z, error = 0, freeblock;
	long blck_read;
	struct inode *inode;
	struct super_block *sb;
	struct memcard *vmudetails;
	struct buffer_head *bh_fat = NULL, *bh;

	if (de->d_name.len > VMUFAT_NAMELEN)
		return -ENAMETOOLONG;

	sb = dir->i_sb;
	vmudetails = sb->s_fs_info;

	inode = new_inode(sb);
	if (!inode) {
		error = -ENOSPC;
		goto out;
	}

	/* Check if this an executible file */
	if (imode & 0111) {
		/* Must be at start of volume */
		inode->i_ino = VMUFAT_ZEROBLOCK;
		/* But this already allocated? */
		if (vmufat_get_fat(sb, 0) != FAT_UNALLOCATED) {
			printk(KERN_ERR
				"VMUFAT: cannot write executible file to"
				" filesystem - block 0 already allocated.\n");
			error = -ENOSPC;
			goto clean_inode;
		}
		freeblock = 0;
	} else {
		/* look for a free block */
		freeblock = vmufat_find_free(sb);
		if (freeblock < 0) {
			error = freeblock;
			goto clean_inode;
		}
		inode->i_ino = freeblock;
	}
	/* mark as single block file - may grow later */
	error = vmufat_set_fat(sb, freeblock, FAT_FILE_END);
	if (error)
		goto clean_inode;

	inode->i_uid = 0;
	inode->i_gid = 0;
	inode->i_mtime = inode->i_atime = inode->i_ctime = CURRENT_TIME;
	inode->i_mode = imode;
	inode->i_blocks = 1;
	inode->i_sb = sb;
	insert_inode_hash(inode);
	inode->i_op = &vmufat_inode_operations;
	inode->i_fop = &vmufat_file_operations;
	inode->i_mapping->a_ops = &vmufat_address_space_operations;

	/* Write to the directory
	* Now search for space for the directory entry */
	blck_read = vmudetails->dir_bnum;
	bh = vmufat_sb_bread(sb, blck_read);
	if (!bh) {
		error = -EIO;
		goto clean_inode;
	}

	for (y = 0; y < (vmudetails->dir_len * DIR_ENT_PER_BLK); y++) {
		if ((y / DIR_ENT_PER_BLK) >
			(vmudetails->dir_bnum - blck_read)) {
			brelse(bh);
			blck_read--;
			bh = vmufat_sb_bread(sb, blck_read);
			if (!bh) {
				error = -EIO;
				goto clean_fat;
			}
		}
		if (((bh->b_data)[vmufat_index(y)]) == 0)
			break;
	}
	/* Have the directory entry
	 * so now update it */
	z = vmufat_index(y);
	if (imode & 0111)
		bh->b_data[z] = VMU_GAME;	/* exec file */
	else
		bh->b_data[z] = VMU_DATA;

	if ((bh->b_data[z + 1] != (char) 0x00) &&
		(bh->b_data[z + 1] != (char) 0xff))
		bh->b_data[z + 1] = (char) 0x00;

	if (inode->i_ino != VMUFAT_ZEROBLOCK) {
		((u16 *) bh->b_data)[z / 2 + 1] =
		    cpu_to_le16(inode->i_ino);
		((u16 *) bh->b_data)[z / 2 + 0x0D] = 0;
	} else {
		((u16 *) bh->b_data)[z / 2 + 1] = 0;
		((u16 *) bh->b_data)[z / 2 + 0x0D] = 1;
	}

	/* Name */
	memset((char *) (bh->b_data + z + 0x04), '\0', 0x0C);
	memcpy((char *) (bh->b_data + z + 0x04), ((de->d_name).name),
		de->d_name.len);

	/* BCD timestamp it */
	vmufat_save_bcd(inode, bh->b_data, z);

	((u16 *) bh->b_data)[z / 2 + 0x0C] =
	    cpu_to_le16(inode->i_blocks);
	mark_buffer_dirty(bh);
	brelse(bh);

	error = vmufat_list_blocks(inode);
	if (error)
		goto clean_fat;

	d_instantiate(de, inode);
	brelse(bh_fat);
	return error;

clean_fat:
	((u16 *)bh_fat->b_data)[freeblock] = cpu_to_le16(FAT_UNALLOCATED);
	mark_buffer_dirty(bh_fat);
	brelse(bh_fat);
clean_inode:
	iput(inode);
out:
	return error;
}

static int vmufat_readdir(struct file *filp, void *dirent, filldir_t filldir)
{
	int filenamelen, i, error = 0;
	struct vmufat_file_info *saved_file = NULL;
	struct dentry *dentry = filp->f_dentry;
	struct inode *inode = dentry->d_inode;
	struct super_block *sb = inode->i_sb;
	struct memcard *vmudetails = sb->s_fs_info;
	struct buffer_head *bh;

	int blck_read = vmudetails->dir_bnum;
	bh = vmufat_sb_bread(sb, blck_read);
	if (!bh) {
		error = -EIO;
		goto out;
	}

	i = filp->f_pos;

	/* handle . for this directory and .. for parent */
	switch ((unsigned int) filp->f_pos) {
	case 0:
		if (filldir(dirent, ".", 1, i++, inode->i_ino, DT_DIR) < 0)
			goto finish;

		filp->f_pos++;
	case 1:
		if (filldir(dirent, "..", 2, i++,
			    dentry->d_parent->d_inode->i_ino, DT_DIR) < 0)
			goto finish;

		filp->f_pos++;
	default:
		break;
	}

	/* trap reading beyond the end of the directory */
	if ((i - 2) > (vmudetails->dir_len * DIR_ENT_PER_BLK)) {
		error = -EINVAL;
		goto release_bh;
	}

	saved_file =
	    kmalloc(sizeof(struct vmufat_file_info), GFP_KERNEL);
	if (!saved_file) {
		error = -ENOMEM;
		goto release_bh;
	}

	do {
		if ((i - 2) / DIR_ENT_PER_BLK >
			(vmudetails->dir_bnum - blck_read)) {
			/* move to next block in directory */
			blck_read--;
			if (vmudetails->dir_bnum - vmudetails->dir_len <=
				blck_read)
				break;
			brelse(bh);
			bh = vmufat_sb_bread(sb, blck_read);
			if (!bh) {
				kfree(saved_file);
				error = -EIO;
				goto out;
			}
		}

		saved_file->ftype = bh->b_data[vmufat_index(i - 2)];

		if (saved_file->ftype == 0)
			break;

		saved_file->fblk =
		    le16_to_cpu(((u16 *) bh->b_data)[1 +
			vmufat_index_16(i - 2)]);
		if (saved_file->fblk == 0)
			saved_file->fblk = VMUFAT_ZEROBLOCK;

		memcpy(saved_file->fname,
		       bh->b_data + 4 + vmufat_index(i - 2), VMUFAT_NAMELEN);
		filenamelen = strlen(saved_file->fname);
		if (filenamelen > VMUFAT_NAMELEN)
			filenamelen = VMUFAT_NAMELEN;
		if (filldir
		    (dirent, saved_file->fname, filenamelen, i++,
		     saved_file->fblk, DT_REG) < 0) {
			goto finish;
		}

		filp->f_pos++;
	} while (1);

finish:
	kfree(saved_file);
release_bh:
	brelse(bh);
out:
	return error;
}

static long vmufat_get_date(struct buffer_head *bh, int offset)
{
	int century, year, month, day, hour, minute, second;

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

	if (!vi)
		return NULL;
	INIT_LIST_HEAD(&vi->blocks.b_list);
	return &vi->vfs_inode;
}

static void vmufat_destroy_inode(struct inode *in)
{
	struct vmufat_inode *vi = VMUFAT_I(in);
	struct vmufat_block_list *vb;
	struct list_head *iter, *iter2;

	list_for_each_safe(iter, iter2, &vi->blocks.b_list) {
		vb = list_entry(iter, struct vmufat_block_list, b_list);
		list_del(iter);
		kmem_cache_free(vmufat_blist_cachep, vb);
	}
	kmem_cache_free(vmufat_inode_cachep, vi);
}

static int vmufat_list_blocks(struct inode *in)
{
	struct vmufat_inode *vi = VMUFAT_I(in);
	struct super_block *sb = in->i_sb;
	long nextblock;
	long ino = in->i_ino;
	struct memcard *vmudetails;
	int error;
	struct list_head *iter, *iter2;
	struct vmufat_block_list *vbl, *nvbl;
	u16 fatdata;

	vmudetails = sb->s_fs_info;
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
		if (fatdata == FAT_UNALLOCATED) {
			printk(KERN_WARNING "VMUFAT: FAT table appears to have"
				" been corrupted.\n");
			error = -EIO;
			goto unwind_out;
		}
		if (fatdata == FAT_FILE_END)
			break;	/*end of file */
		nextblock = fatdata;
	} while (1);

	return 0;

unwind_out:
	list_for_each_entry_safe(vbl, nvbl, &vi->blocks.b_list, b_list) {
		list_del_init(&vbl->b_list);
		kmem_cache_free(vmufat_blist_cachep, vbl);
	}
	return error;
}

static struct inode *vmufat_get_inode(struct super_block *sb, long ino)
{
	struct buffer_head *bh;
	int error, blck_read, y, z;
	struct inode *inode = iget_locked(sb, ino);
	struct memcard *vmudetails = sb->s_fs_info;
	long superblock_bno = vmudetails->sb_bnum;

	if (inode && (inode->i_state & I_NEW)) {
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
				vmufat_get_date(bh, 0x30);

			/* Mark as a directory */
			inode->i_mode = S_IFDIR | S_IRUGO | S_IXUGO;

			inode->i_op = &vmufat_inode_operations;
			inode->i_fop = &vmufat_file_dir_operations;
		} else {
			blck_read = vmudetails->dir_bnum;
			bh = vmufat_sb_bread(sb, blck_read);
			if (!bh) {
				error = -EIO;
				goto failed;
			}

			/* Mark file as regular type */
			inode->i_mode = S_IFREG;

			/* Scan through the directory to find matching file */
			for (y = 0; y < vmudetails->numblocks; y++) {
				if ((y / DIR_ENT_PER_BLK) >
				    (vmudetails->dir_bnum - blck_read)) {
					brelse(bh);
					blck_read--;
					bh = vmufat_sb_bread(sb, blck_read);
					if (!bh) {
						error = -EIO;
						goto failed;
					}
				}
				if (le16_to_cpu(((u16 *) bh->b_data)
					[(y % DIR_ENT_PER_BLK) * 
					DIR_REC_LEN / 2 + 0x01]) == ino)
					break;
				}

				if (y >= vmudetails->numblocks) {
					brelse(bh);
					printk(KERN_INFO
						"vmufat: could not find this "
						"file on filesystem\n");
				error = -ENOENT;
				goto failed;
			}

			/* identified the correct directory entry */
			z = vmufat_index(y);
			inode->i_ctime.tv_sec = inode->i_mtime.tv_sec =
				vmufat_get_date(bh, z + 0x10);

			/* Execute if a game, write if not copy protected */
			inode->i_mode &= ~(S_IWUGO | S_IXUGO);
			inode->i_mode |= S_IRUGO;

			/* Mode - is it write protected? */
			if ((((u8 *) bh->b_data)[0x01 + z] ==
			     0x00) & ~(sb->s_flags & MS_RDONLY))
				inode->i_mode |= S_IWUGO;
			/* Is file executible - ie a game */
			if ((((u8 *) bh->b_data)[z] ==
			     0xcc) & ~(sb->s_flags & MS_NOEXEC))
				inode->i_mode |= S_IXUGO;

			inode->i_fop = &vmufat_file_operations;

			inode->i_blocks =
			    le16_to_cpu(((u16 *) bh->b_data)
				[vmufat_index_16(y) + 0x0C]);
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

	return inode;

failed:
	iget_failed(inode);
	return ERR_PTR(error);
}

static void vmufat_put_super(struct super_block *sb)
{
	sb->s_dev = 0;
	kfree(sb->s_fs_info);
}

static int vmufat_scan(struct super_block *superbl, struct kstatfs *kstatbuf)
{
	int error = 0;
	int free = 0;
	int index;
	u16 fatdata;
	struct buffer_head *bh_fat;
	struct memcard *vmudetails = superbl->s_fs_info;
	long nextblock;

	/* Look through the FAT */
	nextblock = vmudetails->fat_bnum + vmudetails->fat_len - 1;
	index = superbl->s_blocksize;
	bh_fat = vmufat_sb_bread(superbl, nextblock);
	if (!bh_fat) {
		error = -EIO;
		goto out;
	}

	do {
		fatdata = le16_to_cpu(((u16 *) bh_fat->b_data)[index]);
		if (fatdata == FAT_UNALLOCATED)
			free++;
		if (--index < 0) {
			brelse(bh_fat);
			if (--nextblock >= vmudetails->fat_bnum) {
				index = superbl->s_blocksize;
				bh_fat = vmufat_sb_bread(superbl, nextblock);
				if (!bh_fat) {
					error = -EIO;
					goto out;
				}
			} else
				break;
		}
	} while (1);

	buf->f_bfree = free;
	buf->f_bavail = free;
	buf->f_blocks = vmudetails->numblocks;

out:
	return error;
}

static int vmufat_statfs(struct dentry *dentry, struct kstatfs *kstatbuf)
{
	struct super_block *superbl = dentry->d_sb;
	int error;

	error = vmufat_scan(superbl, kstatbuf);
	if (error)
		return error;
	kstatbuf->f_type = VMUFAT_MAGIC;
	kstatbuf->f_bsize = sb->s_blocksize;
	kstatbuf->f_namelen = VMUFAT_NAMELEN;

	return 0;
}

/* Remove inode from memory */
static void vmufat_evict_inode(struct inode *in)
{
	printk(KERN_ERR "Inode is at %i\n", in);
	truncate_inode_pages(&in->i_data, 0);
	invalidate_inode_buffers(in);
	in->i_size = 0;
	end_writeback(in);
}

/*
 * Delete inode by marking space as free in FAT
 * no need to waste time and effort by actually
 * wiping underlying data on media
 */
static void vmufat_remove_inode(struct inode *in)
{
	struct buffer_head *bh, *bh_old;
	struct super_block *sb;
	struct memcard *vmudetails;
	int z, y, x, w, v, blck_read;
	u16 nextblock, fatdata;

	if (in->i_ino == VMUFAT_ZEROBLOCK)
		in->i_ino = 0;
	sb = in->i_sb;
	vmudetails = sb->s_fs_info;
	if (in->i_ino > vmudetails->fat_len * sb->s_blocksize / 2) {
		printk(KERN_ERR "VMUFAT: attempting to delete"
			"inode beyond device size");
		return;
	}

	/* Seek start of file and wander through FAT
 	 * Marking the blocks as unallocated */
	nextblock = in->i_ino;
	do {
		fatdata = vmufat_get_fat(sb, nextblock);
		if (fatdata == FAT_ERROR) 
			goto failure;
		if (vmufat_set_fat(sb, nextblock, FAT_UNALLOCATED))
			goto failure;
		if (fatdata == FAT_FILE_END)
			break;
		nextblock = fatdata;
	} while (1);

	/* Now clean the directory entry
	 * Have to wander through this
	 * to find the appropriate entry */
	blck_read = vmudetails->dir_bnum;
	bh = vmufat_sb_bread(sb, blck_read);
	if (!bh)
		goto failure;

	for (y = 0; y < (vmudetails->dir_len * DIR_ENT_PER_BLK); y++) {
		if ((y / DIR_ENT_PER_BLK) >
			(vmudetails->dir_bnum - blck_read)) {
			brelse(bh);
			blck_read--;
			bh = vmufat_sb_bread(sb, blck_read);
			if (!bh)
				goto failure;
		}
		if (le16_to_cpu(((u16 *) bh->b_data)
			[(y % DIR_ENT_PER_BLK) * DIR_REC_LEN / 2 +
			0x01]) == in->i_ino)
			break;
	}

	/* Found directory entry - so NULL it now */
	w = vmufat_index_16(y);
	for (z = 0; z < DIR_REC_LEN / 2; z++)
		((u16 *) bh->b_data)[w + z] = 0;
	mark_buffer_dirty(bh);
	/* Replace it with another entry - if one exists */
	x = y;
	for (y = x + 1; y < (vmudetails->dir_len * DIR_ENT_PER_BLK); y++) {
		if ((y / DIR_ENT_PER_BLK) >
			(vmudetails->dir_bnum - blck_read)) {
			brelse(bh);
			blck_read--;
			bh = vmufat_sb_bread(sb, blck_read);
			if (!bh)
				return;
		}
		/* look for the end of entries in the directory */
		if (bh->b_data[vmufat_index(y)] == 0) {
			y--;
			if (y == x)
				break;	/* At the end in any case */
			brelse(bh);

			/* force read of correct block */
			bh = vmufat_sb_bread(sb, vmudetails->dir_bnum -
				y / DIR_ENT_PER_BLK);
			if (!bh)
				goto failure;
			bh_old =
			    vmufat_sb_bread(sb, vmudetails->dir_bnum -
				x / DIR_ENT_PER_BLK);
			if (!bh_old) {
				brelse(bh);
				goto failure;
			}

			/*
 			 * Copy final directory entry into space created
			 * by the deletion of the inode
			 */
			w = vmufat_index_16(x);
			v = vmufat_index_16(y);
			for (z = 0; z < DIR_REC_LEN / 2; z++) {
				((u16 *) bh_old->b_data)[w + z] =
					((u16 *) bh->b_data)[v + z];
				((u16 *) bh->b_data)[v + z] = 0;
			}
			mark_buffer_dirty(bh);
			/* check if the same buffer */
			if (x / DIR_ENT_PER_BLK != y / DIR_ENT_PER_BLK)
				mark_buffer_dirty(bh_old);
			brelse(bh_old);
			break;
		}
	}
//	vmufat_evict_inode(in);
	brelse(bh);
	return;

failure:
	printk(KERN_ERR "VMUFAT: Failure to read volume,"
		" could not delete inode - filesystem may be damaged\n");
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
	if (!in)
		return -EIO;
	vmufat_remove_inode(in);
	return 0;
}

static int vmufat_get_block(struct inode *inode, sector_t iblock,
	struct buffer_head *bh_result, int create)
{
	struct vmufat_inode *vin = VMUFAT_I(inode);
	struct vmufat_block_list *vlist = &vin->blocks;
	struct vmufat_block_list *vblk;
	struct super_block *sb = inode->i_sb;
	int cural;
	int finblk, nxtblk, exeblk;
	struct list_head *iter;
	sector_t cntdwn = iblock;
	sector_t phys;
	int error = -EINVAL;

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
	if (!create) {
		error = -EINVAL;
		goto out;
	}
	/*
	 * check not looking for a block too far
	 * beyond the end of the existing file
	 */
	if (iblock > vin->nblcks) {
		error = -EINVAL;
		goto out;
	}

	/* if looking for a block that is not current - allocate it*/
	cural = vin->nblcks;
	list_for_each(iter, &vlist->b_list) {
		if (cural-- == 1)
			break;
	}
	vblk = list_entry(iter, struct vmufat_block_list, b_list);
	finblk = vblk->bno;

	/* Exec files have to be linear */
	if (inode->i_ino == 0) {
		exeblk = vmufat_get_fat(sb, finblk + 1);
		if (exeblk != FAT_UNALLOCATED) {
			printk(KERN_WARNING "VMUFAT: Cannot allocate linear "
				"space needed for executible\n");
			error = -ENOSPC;
			goto out;
		}
		nxtblk = finblk + 1;
	} else {
		nxtblk = vmufat_find_free(sb);
		if (nxtblk < 0) {
			error = nxtblk;
			goto out;
		}
	}
	error = vmufat_set_fat(sb, finblk, nxtblk);
	if (error)
		goto out;
	error = vmufat_set_fat(sb, nxtblk, FAT_FILE_END);
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
	struct buffer_head *bh;
	unsigned long inode_num;
	int y, blck_read, z;
	struct super_block *sb = in->i_sb;
	struct memcard *vmudetails =
	    ((struct memcard *) sb->s_fs_info);

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
	blck_read = vmudetails->dir_bnum;
	bh = vmufat_sb_bread(sb, blck_read);
	if (!bh)
		return -EIO;

	for (y = 0; y < vmudetails->numblocks; y++) {
		if ((y / 0x10) > (vmudetails->dir_bnum - blck_read)) {
			brelse(bh);
			blck_read--;
			bh = vmufat_sb_bread(sb, blck_read);
			if (!bh)
				return -EIO;
		}
		if (le16_to_cpu
		    (((__u16 *) bh->b_data)[vmufat_index_16(y) +
					    0x01]) == inode_num)
			break;
	}
	/* Have the directory entry
	 * so now update it */
	z = (y % 0x10) * 0x20;
	if (inode_num != 0)
		bh->b_data[z] = VMU_DATA;	/* data file */
	else
		bh->b_data[z] = VMU_GAME;
	if (bh->b_data[z + 1] !=  0
	    && bh->b_data[z + 1] != (char) 0xff)
		bh->b_data[z + 1] = 0;
	((__u16 *) bh->b_data)[z / 2 + 1] = cpu_to_le16(inode_num);

	/* BCD timestamp it */
	in->i_mtime = CURRENT_TIME;
	vmufat_save_bcd(in, bh->b_data, z);

	((__u16 *) bh->b_data)[z / 2 + 0x0C] = cpu_to_le16(in->i_blocks);
	if (inode_num != 0)
		((__u16 *) bh->b_data)[z / 2 + 0x0D] = 0;
	else /* game */
		((__u16 *) bh->b_data)[z / 2 + 0x0D] = cpu_to_le16(1);
	mark_buffer_dirty(bh);
	brelse(bh);
	return 0;
}

static int check_sb_format(struct buffer_head *bh)
{
	u32 s_magic = VMUFAT_MAGIC;

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
};

static const struct file_operations vmufat_file_operations = {
	.llseek =	generic_file_llseek,
	.read =		do_sync_read,
	.write =	do_sync_write,
	.aio_read =	generic_file_aio_read,
	.aio_write =	generic_file_aio_write,
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
